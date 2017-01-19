Question 1
----------
For pairs, I used 2 MapReduce jobs. The first job I read the input and produced an output file of the frequency of words appeared each line and I used stars to keep track of total file length. The second job, I first got all unique words and then I paired them together and sent them to the reducer which computes the pmi and output the pair with pmi and count. The input records for first job is the shakespear file and same to second job. However in the setup step of reducer of second job, I read in the second job input to contruct a map of all words and count with total lines. Intermediate key-value pair are the key are each unique words and value is the number of lines it occured on and the total lines count. The final output records is a key of co-occurring pair followed by the value of pair with the pair pmi and count of co-occurence.

For stripes, I used 2 MapReduce jobs. The first job I read the input and produced an output file of the frequency of words appeared each line and I used stars to keep track of total file length. The second job, I first got all unique words and send it to reducer as a key and map of all values. The in reducer i add them and then calulates every pmi by looping the map and output eveything as a hashmap. The input records for first job is the shakespear file and same to second job. However in the setup step of reducer of second job, I read in the second job input to contruct a map of all words and count with total lines.Intermediate key-value pair are the key are each unique words and value is the number of lines it occured on and the total lines count. The final output records is a key of the first word of pair followed by a hashmap with a key as second word of pair and the value is the pmi of the pair and the count of the occurence.

Question 2
----------
Ran on linux.student.cs.uwaterloo.ca
Pairs - Count Job Finished in 7.008 seconds + Pair Job Finished in 53.67 seconds = 60.678 seconds total
Stripes - Count Job Finished in 9.041 seconds + Pair Job Finished in 18.429 seconds = 27.47 seconds total

Question 3
----------
Ran on linux.student.cs.uwaterloo.ca
Pairs - Count Job Finished in 9.007 seconds + Pair Job Finished in 56.63 seconds = 65.637 seconds total
Stripes - Count Job Finished in 8.987 seconds + Pair Job Finished in 23.459 seconds = 32.446 seconds total

Question 4
----------
  77198  308792 3002124

Question 5
----------
(highest PMI)
(maine, anjou)  (3.6331423021951013, 12)
(anjou, maine)  (3.6331423021951013, 12)

(lowest PMI)
(thy, you)      (-1.5303967668481644, 11)
(you, thy)      (-1.5303967668481644, 11)

Question 6
----------
('tears')
(tears, shed)   (2.111790076876236, 15)
(tears, salt)   (2.0528122169168985, 11)
(tears, eyes)   (1.1651669643071034, 23)

('death')
(death, father's)       (1.1202520304197314, 21)
(death, die)    (0.7541593889996885, 18)
(death, life)   (0.7381345918721788, 31)

Question 7
----------

Question 8
----------

