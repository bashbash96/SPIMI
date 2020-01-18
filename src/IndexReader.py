import os
from Dictionary import Dictionary
from PostingList import PostingList
from IndexWriter import IndexWriter
import time

PATH = "{}\{}.txt"
COMPRESSION_TYPE = 'FC'
COMPRESSION_BLOCKS = 1000
VARIANT_ENCODE_TYPE = 'V'
DICTIONARY_FILE_NAME = 'Dictionary'
POSTING_LISTS_FILE_NAME = 'PostingLists'
FC_DATA_FILE_NAME = 'FCData'
DOCS_FREQ_FiLE_NAME = 'DocsFreq'
TERMS_FREQ_FILE_NAME = 'TermsFreq'
TERMS_FREQ_POINTERS_FILE_NAME = 'TermsFreqPointers'
POSTING_LISTS_POINTERS_FILE_NAME = 'PostingListsPointers'
DOCS_NUMBER_FILE_NAME = 'NumberOfDocs'


def getListFromGaps(gaps):
    """
    private function to get list of nubmers from its gaps list
    :param gaps: list of gaps
    :return:
    """
    res = []
    for i, val in enumerate(gaps):
        if i == 0:
            res.append(val)
        else:
            res.append(res[i - 1] + val)

    return res


class IndexReader:
    def __init__(self, dir):
        """Creates an IndexReader which will read from
            the given directory
            dir is the name of the directory in which all
            index files are located."""
        if not(os.path.isdir(dir)):
            print('Error - Invalid directory name')
            exit(0)

        self.indexDir = dir
        self.numberOfDocs = 0
        self.listOfTerms = []
        self.dictionary = {}
        self.postingLists = []
        self.buildIndexReader()

    def buildIndexReader(self):
        self.numberOfDocs = self.getNumberOfDocsFromDisk()
        self.dictionary = self.getDictionaryFromFiles()

    def getDictionaryFromFiles(self):
        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME )
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME)
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME)
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME )
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME)

        if not (os.path.isfile(DictionaryPath)) or not (os.path.isfile(FCDataPath))\
                or not (os.path.isfile(DocsFreqPath)) or not (os.path.isfile(PostingListsPointersPath))\
                or not (os.path.isfile(TermsFreqPointersPath)):
            print('Error - Invalid File Path! Please Enter a Valid Path..')
            exit(0)
        dict, termsStr, fcData, docsFreq, postingListsPointers, termsFreqPointers = {}, '', [], [], [], []

        FCObj = Dictionary([], (COMPRESSION_TYPE, COMPRESSION_BLOCKS))
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)

        with open(DictionaryPath, 'r') as dictFid, open(FCDataPath, 'rb') as fcDataFid:
            termsStr = dictFid.read()
            fcDataStream = fcDataFid.read()
            fcData = encodeObj.variantDecode(fcDataStream)

        with open(DocsFreqPath, 'rb') as docsFreqFid:
            docsFreqStream = docsFreqFid.read()
            docsFreq = encodeObj.variantDecode(docsFreqStream)

        with open(PostingListsPointersPath, 'rb') as postingListsPointersFid, open(TermsFreqPointersPath,
                                                                                   'rb') as termsFreqPointersFid:
            postingListsPointersStream = postingListsPointersFid.read()
            termsFreqPointersStream = termsFreqPointersFid.read()
            postingListsPointers = encodeObj.variantDecode(postingListsPointersStream)
            termsFreqPointers = encodeObj.variantDecode(termsFreqPointersStream)

            postingListsPointers = getListFromGaps(postingListsPointers)
            termsFreqPointers = getListFromGaps(termsFreqPointers)

        fcData = self.getFCDataFromFile(fcData)
        terms = []
        FCObj.str = termsStr
        for block in fcData:
            terms.extend(FCObj.getListOfWords(block))
        self.listOfTerms = terms
        for i, term in enumerate(terms):
            dict[term] = [docsFreq[i], -1, postingListsPointers[i], termsFreqPointers[i]]

        return dict

    def getFCDataFromFile(self, lst):
        dict = []
        i = 0
        while i < len(lst):
            currTuple = (lst[i],)
            j = i + 1
            while j < len(lst) and j < i + 2 * COMPRESSION_BLOCKS:
                pair = (lst[j], lst[j + 1])
                currTuple += (pair,)
                j += 2
            dict.append(currTuple)
            i = i + 2 * COMPRESSION_BLOCKS + 1

        return dict

    def getNumberOfDocsFromDisk(self):
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)

        NumberOfDocsPath = PATH.format(self.indexDir, DOCS_NUMBER_FILE_NAME )

        if not (os.path.isfile(NumberOfDocsPath)):
            print('Error - Invalid File Path! Please Enter a Valid Path..')
            exit(0)

        with open(NumberOfDocsPath, 'rb') as numberOfDocsFid:
            numberOfDocsStream =  numberOfDocsFid.read()
            numberOfDocsStream = encodeObj.variantDecode(numberOfDocsStream)
            numberOfDocs = numberOfDocsStream[0]

        return numberOfDocs

    def getTermIdxFromSortedList(self, terms, token):
        start, end = 0, len(terms) - 1

        while start <= end:
            mid = (start + end) // 2
            if token > terms[mid]:
                start = mid + 1
            elif token < terms[mid]:
                end = mid - 1
            else:
                return mid
        return -1

    def getSpecifTokenPostingList(self, token):
        dict, terms = self.dictionary, self.listOfTerms

        idx = self.getTermIdxFromSortedList(terms, token)
        start = idx
        if idx == len(terms) - 1:
            end = start
        else:
            end = idx + 1

        TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME)
        PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME)

        postingStartSeek = dict[terms[start]][2]
        postingEndSeek = dict[terms[end]][2]
        termsStartSeek = dict[terms[start]][3]
        termsEndSeek = dict[terms[end]][3]
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)

        with open(TermsFreqPath, 'rb') as termsFreqFid, open(PostingListsPath, 'rb') as postingListsFid:
            termsFreqFid.seek(termsStartSeek)
            if end == len(terms) - 1:
                termsFreq = termsFreqFid.read()
            else:
                termsFreq = termsFreqFid.read(termsEndSeek - termsStartSeek)
            termsFreq = encodeObj.variantDecode(termsFreq)
            postingListsFid.seek(postingStartSeek)
            if end == len(terms) - 1:
                postingLists = postingListsFid.read()
            else:
                postingLists = postingListsFid.read(postingEndSeek - postingStartSeek)
            postingLists = encodeObj.variantDecode(postingLists)


        postingLists = getListFromGaps(postingLists)

        res = []
        for i in range(dict[token][0]):
            res.append([postingLists[i], termsFreq[i]])

        return res

    def getTokenFrequency(self, token):
        """
        Return the number of documents containing a given token (i.e., word)
        Returns 0 if there are no documents containing this token
        :param token: a certain token
        :return: number of documents
        """
        if token in self.dictionary:
            return self.dictionary[token][0]
        return 0

    def getTokenCollectionFrequency(self, token):
        """
        Return the number of times that a given token (i.e., word) appears
        in the whole collection.
        Returns 0 if there are no documents containing this token
        :param token: a certain token
        :return: the number of appearances of a certain token in the whole collection
        """
        if token in self.dictionary:
            if self.dictionary[token][1] != -1:
                return self.getTokenCollectionFreq(token)
            else:
                self.getPostingListFromDisk(token)
                return self.getTokenCollectionFreq(token)
        return 0

    def getPostingListFromDisk(self, token):
        postingList = self.getSpecifTokenPostingList(token)
        self.dictionary[token][1] = len(self.postingLists)
        self.postingLists.append(postingList)

    def getTokenCollectionFreq(self, token):
        sum = 0
        for pair in self.postingLists[self.dictionary[token][1]]:
            sum += pair[1]
        return sum

    def getDocsWithToken(self, token):
        """
        Returns a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
        that id-n is the n-th document containing the given token and freq-n is the
        number of times that the token appears in doc id-n
        Note that the integers should be sorted by id.
        Returns an empty Tuple if there are no documents containing this token
        :param token: a certain token
        :return: series of pairs of all docs and token frequency of that docs
        """
        if token in self.dictionary:
            if self.dictionary[token][1] != -1:
                return self.getTokenPairs(token)
            else:
                self.getPostingListFromDisk(token)
                return self.getTokenPairs(token)
        return ()

    def getTokenPairs(self, token):
        res = ()
        for pair in self.postingLists[self.dictionary[token][1]]:
            res += ((pair[0], pair[1]),)
        return res

    def getNumberOfDocuments(self):
        """
        Return the number of documents in the collection
        :return: the number of documents in the collection
        """
        return self.numberOfDocs
