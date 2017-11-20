f = open("test_colon_s3.csv",'r')
ft = open("write_test_colon.csv",'w')

count = 0
for item in f.readlines():
	if count is not 0:
		p = item.find(',')
		s = item[p+1:].strip('\n')
		t = item[:p]
		s = s + ',' + t + '\n'
		ft.write(s)
	count = count + 1