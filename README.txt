                       Index Writer, Reader and Searcher
                   ==========================================================
                            CREATED BY : AMJAD BASHITI

Program description :
a project to implement the SPIMI Algorithm for building index and writing it on the disk, another 
program is to read the writed index and get information about it, the last program is to get specific
AND queries from the index and rank them due to tf-idf ranking.

Program files : 
the program has five file, IndexWriter.py to write the index of the disk, IndexReader.py to get information 
about the writed index, IndexSearcher.py to search specific queries in the index, PostingList.py to 
compress specific term posting list or list of numbers with variant encode algorithm, Dictionary.py 
to compress list of terms with front coding algorithm.

Compile and running : 
you should make an instance of IndexWriter with name of dir to store the index and name of text file
with the input (which is in format of docs splited with 80 stars).
*******
when you make this instance it's very important to run it under the CONDITION "" if __name__ == __main__: "" because
we are using multiprocessing and it's not working without this condition 
*******
then you can get information by making instance of IndexReader, and lastly search for queries by making
instance of IndexSearcher
