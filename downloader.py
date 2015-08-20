import urllib2, threading, re, os, sys, time, pdb

# in this version a writer thread is added, downloaded sections will be written into disk periodically, so that memory usage keeps stable
DEBUG = True

section_id = 0
finish_section_id = 0
section_lock = threading.Lock()
result_lock = threading.Lock()

def download_section(url, (start, end), (result, id)):
	global result_lock
	global DEBUG
	get_request = urllib2.Request(url)
	get_request.add_header('range', 'bytes=%d-%d' % (start, end))

	while True:
		try:
			section = urllib2.urlopen(get_request, timeout = 10)
			content = section.read()
			
			result_lock.acquire()
			result[id] = content
			result_lock.release()
			
			break
		except Exception, e:
			if DEBUG: print e
			
		if DEBUG: print "retry on section", id + 1
		time.sleep(0.1)
		
	if DEBUG: print "finish downloading section", id + 1

def download_wrapper(url, n_thread, sections, result):
	global section_id
	global finish_section_id
	global section_lock
	global DEBUG
	
	while True:
		section_lock.acquire()
		
		if section_id == len(sections):
			section_lock.release()
			break
		
		task_id = section_id
		if DEBUG: print "start downloading section", task_id + 1, "(bytes=%d-%d)" % sections[task_id]
			
		section_id += 1
		section_lock.release()
		
		download_section(url, sections[task_id], (result, task_id))	
		
		wait_time = max(0, section_id - finish_section_id - n_thread)
		if DEBUG: print "waiting for previous sections for %d seconds" % (wait_time)
		time.sleep(wait_time)
		
def format_time(seconds):
	seconds = int(seconds)
	
	d = seconds / 86400
	seconds %= 86400
	days = (d > 0) and "%dd " % (d) or ''
	
	h = seconds / 3600
	seconds %= 3600
	hours = (h > 0) and "%dh " % (h) or ''
	
	m = seconds / 60
	seconds %= 60
	minutes = (m > 0) and "%dm " % (m) or ''
	
	formatted_time = days + hours + minutes + "%ds" % (seconds)
	return formatted_time

def file_writer(name, result, sections):
	global finish_section_id
	global result_lock
	global DEBUG
	
	n_section = len(sections)
	n_finish = 0
	time_passed = 0
	
	while n_finish < n_section:
		start_time = time.time()
		
		n_finish_old = n_finish
		result_lock.acquire()
		for i in range(n_finish, n_section):
			if result[i] != '':
				n_finish += 1
				if DEBUG: print "saving section %d/%d" % (i + 1, n_section)
			else:
				break
		result_lock.release()
		
		finish_section_id = n_finish
		
		if n_finish > n_finish_old:
			output = ''
			
			while output == '':
				try:
					output = open(name + ".tmp", "ab")
				except Exception, e:
					if DEBUG: print e
				time.sleep(5)
			
			result_lock.acquire()
			for i in range(n_finish_old, n_finish):
				output.write(result[i])
				result[i] = ''
			
			result_lock.release()
			output.close()
			
			finish_length = sections[n_finish - 1][1] + 1
			open(name + ".cfg", "a").write("finish_length:%d\n" % (finish_length))
			
			print "%.2f%% finished |" % (finish_length * 100 / float(sections[-1][1])),
			remaining_time = format_time((n_section - n_finish) * time_passed / n_finish)
			print "%s remaing" % (remaining_time)
		
		
		time.sleep(1)
		time_passed += time.time() - start_time

def download(filename, url, n_thread, sections):
	start_time = time.time()
		
	result = [''] * len(sections)
	threads = []
	
	for i in range(n_thread):
		t = threading.Thread(target = download_wrapper, args = (url, n_thread, sections, result))
		threads.append(t)
		t.start()
		
	t = threading.Thread(target = file_writer, args = (filename, result, sections))
	threads.append(t)
	t.start()
	
	for t in threads:
		t.join()
	
	end_time = time.time()
	print "download time: %s" % format_time(end_time - start_time)
	print "download speed %.2fKB/s" % ((sections[-1][1] - sections[0][0]) / 1000 / (end_time - start_time))
	
	os.rename(filename + ".tmp", filename)
	os.remove(filename + ".cfg")	

def analyze_url(url, retry_max = 10):
	global DEBUG
	head_request = urllib2.Request(url)
	#head_request.get_method = lambda: "HEAD"
	
	while retry_max > 0:
		try:
			response = urllib2.urlopen(head_request, timeout = 10)
			length = int(response.info()['content-length'])
			url = response.geturl()
			name = url.split("/")[-1][:50]
			
			# if filename is specified in Content-Disposition field, use it
			if "Content-Disposition" in response.info():
				match = re.search('filename="(.*)"', response.info()["Content-Disposition"])
				if len(match.groups()) > 0:
					name = match.groups()[0].decode("utf-8")
			
			return length, url, name
		
		except urllib2.HTTPError, e:
			if DEBUG: print e
			retry_max -= 1
		time.sleep(0.5)
					
	return 0, '', ''
	
def init_config_file(filename, url, n_thread, section_size):
	record = open(filename + ".cfg", "w")
	record.write("url:%s\n" % (url))
	record.write("n_thread:%d\n" % (n_thread))
	record.write("section_size:%d\n" % (section_size))
	record.write("finish_length:0\n")
	record.close()

def read_config_file(filename):
	config = {}
	lines = open(filename + ".cfg", "r").readlines()
	
	for line in lines:
		parts = line[:-1].split(":")
		config_name = parts[0]
		config_value = ":".join(parts[1:])
		
		if not config_name in config:
			config[config_name] = []
			
		config[config_name].append(config_value)
	
	return config
	
def init_download_task(url, n_thread = 10, section_size = 500000, start_position = 0):
	length, real_url, filename = analyze_url(url)	
	section_size = min(section_size, length / n_thread + 1)	
	
	if length == 0:
		print "The url is not available! Stop downloading."
		return
		
	if not os.path.exists(filename + '.cfg'):	
		init_config_file(filename, url, n_thread, section_size)
	else:
		# part of the file has been downloaded, read config and continue downloading
		config = read_config_file(filename)
		start_position = int(config["finish_length"][-1])
		print "continue downloading %s from length %d" % (filename, start_position)
	
	start = start_position
	sections = []
	
	while start < length:
		end = min(length, start + section_size - 1)								
		sections.append((start, end))
		start += section_size
	
	print "downloading from", real_url
	print "total length: %d, remaining sections: %d" % (length, len(sections))
	
	download(filename, real_url, n_thread, sections)

def continue_download_task(filename):
	if not os.path.exists(filename + '.cfg'):
		return
	
	config = read_config_file(filename)
	url = config["url"][0]
	n_thread = int(config["n_thread"][0])
	section_size = int(config["section_size"][0])
	start_position = int(config["finish_length"][-1])
	
	init_download_task(url, n_thread, section_size, start_position)

def main():
	url = "http://bj.dl.baidupcs.com/file/62ba1045aeaedc4cae37820e146cd7cb?bkt=p-fc5616481ca8895a8fc780e00518ba4a&fid=36914600-250528-2903025603&time=1439070799&sign=FDTAXERBH-DCb740ccc5511e5e8fedcff06b081203-46ZqwHjIP8KbG6IB47ItEsWpVCU%3D&to=abp&fm=Qin,B,T,e&sta_dx=215&sta_cs=0&sta_ft=7z&sta_ct=7&fm2=Qingdao,B,T,e&newver=1&newfm=1&secfm=1&flow_ver=3&expires=8h&rt=pr&r=829711843&mlogid=4074389455&vuk=36914600&vbdid=1315256616&fin=Minecraft1.6.4%20Pixelmon%E7%AE%80%E5%8D%95%E6%95%B4%E5%90%88%E7%89%88%E6%9C%AC.7z&fn=Minecraft1.6.4%20Pixelmon%E7%AE%80%E5%8D%95%E6%95%B4%E5%90%88%E7%89%88%E6%9C%AC.7z&slt=pm&uta=0&rtype=1&iv=0&isw=0"
	url = "http://www.uv.es/gonzalev/PSI%20ORG%2006-07/ARTICULOS%20RRHH%20SOCIOTEC/Trist%20Long%20Wall%20Method%20HR%201951.pdf"
	#url = "http://www.saeedsh.com/resources/Thinking%20in%20Java%204th%20Ed.pdf"
	init_download_task(url, 10, 500000)	
	
if __name__ == '__main__':
	main()