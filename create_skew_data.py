# create_skew_data.py


''' 
    Loops from a-z which will be our keys, for each on one table we will create an even distribution
    For the second table we will create a bunch of skew on one key just to create a problem. 
'''

file_1 = open("dataset_a", "a")
file_2 = open("dataset_b", "a")
for key in range(97,123):
    print(chr(key))
    #Create data set 1
    file_1.write("{k},{v}\n".format(k=chr(key),v='MyValue' + str(chr(key))))
    
    #Create data set 1
    if chr(key) == 'z':
        for val2 in range(1,50000001):
            file_2.write("{k},{v}\n".format(k=chr(key),v=val2))
    else:
        for val3 in range(1,100001):
            file_2.write("{k},{v}\n".format(k=chr(key),v=val3))
file_1.close()
file_2.close()



'''
#Put the files into hdfs

hdfs dfs -mkdir -p /data

hdfs dfs -put dataset_a /data/
hdfs dfs -put dataset_b /data/
hdfs dfs -ls /data/



#Remove the files from local disk
rm -r dataset_a
rm -r dataset_b
#Cleanup from hdfs when neeeded
hdfs dfs -rm -r -skipTrash /data/dataset_a
hdfs dfs -rm -r -skipTrash /data/dataset_b
'''

