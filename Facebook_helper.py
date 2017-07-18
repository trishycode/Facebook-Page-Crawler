import csv, os


with open('seed.csv', 'r') as csvfile: 
	reader = csv.reader(csvfile, delimiter=',', quotechar = '|')
	for row in reader:
		target = row[0]
		startDateTime = row[1]
		endDateTime = row[2]
		script = 'python3 Facebook_Page_Crawler.py \'' + target + '\' \''+startDateTime+'\' \''+ endDateTime +'\' -r yes'
		print(script)
		try:
			os.system(script)
		except IOError:
			print("Entry invalid; check specified time range")
		#print (row)		





# myfile = open("seed.csv", 'r')
# reader = csv.reader(myfile)


# for row in reader:
#     if rownum == 0:
#         header = row
#         # depending upon the format of the csv file you might have to split things as well, if the file is not formatted properly, which generally is the case. 

#     else:
#         colnum = 0
#         for col in row:
#             print ("%-8s: %s" % (header[colnum], col))
#             colnum += 1
#     rownum += 1
# ifile.close()
