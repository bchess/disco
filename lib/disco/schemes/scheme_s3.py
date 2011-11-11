import cStringIO as StringIO
import Queue
import collections
import gzip
import hashlib
import threading
import urlparse

import boto


CHUNK_SIZE = 8 * 1024 * 1024
NUMBER_OF_DOWNLOAD_THREADS = 16


class S3Stream(object):
	def __init__(self, stream, size, url, params):
		self.url = url
		self.set_bucket_and_key_names(url)
		self.aws_access_key_id = params['aws_access_key_id']
		self.aws_secret_access_key = params['aws_secret_access_key']
		self.chunk_iterator = None
		self.fake_position = -1
		self.eof = False

	def __iter__(self):
		buffer = ''
		while buffer or not self.eof:
			lines = buffer.split('\n')
			for line in lines[:-1]:
				yield line + '\n'
			if self.eof:
				if lines[-1]:
					yield lines[-1]
				break
			buffer = lines[-1] + self.read(1024)

	def set_bucket_and_key_names(self, s3_uri):
		s3_uri = urlparse.urlparse(s3_uri)

		assert s3_uri.scheme == 's3'
		assert s3_uri.params == ''
		assert s3_uri.query == ''
		assert s3_uri.fragment == ''

		self.bucket_name = s3_uri.netloc
		self.key_name = s3_uri.path.lstrip('/')

	def size(self):
		return self.retrieve_key().size

	def retrieve_key(self):
		s3_connection = boto.connect_s3(
			aws_access_key_id=self.aws_access_key_id,
			aws_secret_access_key=self.aws_secret_access_key
		)
		bucket = s3_connection.get_bucket(self.bucket_name)
		return bucket.get_key(self.key_name)

	def iter_byte_ranges(self, chunk_size, total_size):
		for start in xrange(0, total_size, chunk_size):
			end = min(total_size, start + chunk_size) - 1
			yield 'bytes=%s-%s' % (start, end)

	def retrieve_chunk(self, byte_range, position, callback):
		key = self.retrieve_key()
		result = key.get_contents_as_string(headers={'Range': byte_range})
		callback(position, result)

	def queue_retrieval(self, key, output_queue):
		positions_completed = set()
		positions_completed_queue = Queue.Queue()
		contiguos_positions_completed_count = 0

		def chunk_completed_callback(position, result):
			positions_completed_queue.put(position)
			output_queue.put((position, result))

		threads = []
		for i, byte_range in enumerate(self.iter_byte_ranges(CHUNK_SIZE, key.size)):
			# If a chunk takes a long time to download, then many chunks after
			# it could be consuming tons of memory waiting for it. To avoid
			# this, don't start any new threads while we're too far behind.
			while contiguos_positions_completed_count + NUMBER_OF_DOWNLOAD_THREADS < i:
				positions_completed.add(positions_completed_queue.get())
				while contiguos_positions_completed_count in positions_completed:
					positions_completed.remove(contiguos_positions_completed_count)
					contiguos_positions_completed_count += 1
			thread = threading.Thread(target=self.retrieve_chunk, args=(byte_range, i, chunk_completed_callback))
			# thread.daemon = True
			threads.append(thread)
			thread.start()
		for thread in threads:
			thread.join()
		output_queue.put(None)

	def iter_chunks(self):
		chunk_queue = Queue.Queue(2 * NUMBER_OF_DOWNLOAD_THREADS)
		key = self.retrieve_key()

		threading.Thread(target=self.queue_retrieval, args=(key, chunk_queue)).start()

		hash = hashlib.md5()
		position_to_chunks = {}
		next_chunk = 0

		for position, chunk in iter(chunk_queue.get, None):
			position_to_chunks[position] = chunk
			while True:
				chunk_contents = position_to_chunks.pop(next_chunk, None)
				if chunk_contents is None:
					break
				hash.update(chunk_contents)
				yield chunk_contents
				next_chunk += 1

		assert key.etag.strip('"') == hash.hexdigest()

	def tell(self):
		if not self.eof:
			self.fake_position -= 1
		return self.fake_position

	def seek(self, offset, whence=0):
		return None

	def retrieve_one_more_read_buffer(self):
		try:
			next_buffer = self.chunk_iterator.next()
			self.read_buffers.append(next_buffer)
			return len(next_buffer)
		except StopIteration:
			self.eof = True
			return 0

	def read(self, size=None):
		# hack hack hack hack hack hack hack
		# hack hack hack hack hack hack hack
		if self.eof:
			return self.last_eight.read(size)
		# hack hack hack hack hack hack hack
		# hack hack hack hack hack hack hack

		if self.chunk_iterator is None:
			self.chunk_iterator = self.iter_chunks()
			self.read_buffers = collections.deque()
			self.index_into_first_buffer = 0

		if self.eof:
			return ''
		elif size is None:
			result = []
			if self.read_buffers:
				first_buffer = self.read_buffers.popleft()
				result.append(first_buffer[self.index_into_first_buffer:])
				result.extend(self.read_buffers)
			result.extend(self.chunk_iterator)

			self.read_buffers = collections.deque()
			self.index_into_first_buffer = 0
			self.eof = True
		else:
			result = []
			len_of_result = 0
			len_of_read_buffers = 0
			for read_buffer in self.read_buffers:
				len_of_read_buffers += len(read_buffer)

			while len_of_read_buffers < size and not self.eof:
				len_of_read_buffers += self.retrieve_one_more_read_buffer()

			while self.read_buffers and len_of_result < size:
				next_buffer = self.read_buffers[0]
				if len_of_result + len(next_buffer) - self.index_into_first_buffer <= size:
					self.read_buffers.popleft()
					result.append(next_buffer[self.index_into_first_buffer:])
					len_of_result += len(next_buffer) - self.index_into_first_buffer
					self.index_into_first_buffer = 0
				else:
					len_needed = size - len_of_result
					result.append(next_buffer[self.index_into_first_buffer:self.index_into_first_buffer + len_needed])
					len_of_result += len_needed
					self.index_into_first_buffer += len_needed

			if not self.read_buffers and not self.eof:
				self.retrieve_one_more_read_buffer()

		result = ''.join(result)
		if self.eof:
			self.last_eight = StringIO.StringIO(result[-8:])
		return result


def input_stream(*args, **kwargs):
	s3_stream = S3Stream(*args, **kwargs)
	if s3_stream.url.endswith('.gz'):
		stream = gzip.GzipFile(fileobj=s3_stream, mode='r')
		size = -1
	else:
		stream = s3_stream
		size = s3_stream.size()
	return stream, size, s3_stream.url
