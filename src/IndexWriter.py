import os
import shutil
from Dictionary import Dictionary
from PostingList import PostingList
from multiprocessing import Process, Lock, Manager

SPLIT_SIGN = '*' * 80 + '\n'
BLOCK_SIZE = 1147483647

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


def getMatches(phrase):
    """
    private function to get all the matches strings from the phrase according to
    list of legal chars and make them lower case
    :param phrase: string
    :return: array of all the matches
    """
    listOfLegalChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890'
    match = []

    currWord = ''
    for i in range(len(phrase)):
        if phrase[i] in listOfLegalChars:
            currWord = currWord + phrase[i]
        elif currWord != '':
            match.append(currWord.lower())
            currWord = ''

    if currWord != '':
        match.append(currWord.lower())

    return match


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


class IndexWriter:
    def __init__(self, inputFile, dir):
        """Given a collection of documents, creates an on disk index
        inputFile is the path to the file containing the review data
        (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""
        if __name__ == 'IndexWriter' or __name__ == '__main__':
            self.indexInputFile = inputFile
            self.indexDir = dir
            self.dictionary = {}
            self.postingList = {}
            self.numberOfDocs = 0
            self.indexOutPutFile = ''

            self.currBlockNum = 1

            self.buildIndex()

    def buildIndex(self):
        """a private method to build the index on the disk..."""
        if not (os.path.isfile(self.indexInputFile)):
            print('Error - Invalid File Path! Please Enter a Valid Path..')
            exit(0)

        docID, currBlockNum, blocksList = 0, 1, []
        #   Read all blocks according to Block Size
        with open(self.indexInputFile, buffering=BLOCK_SIZE) as fid:
            while True:
                # start building new block
                dictionary, postingLists = {}, []
                lines = fid.readlines(BLOCK_SIZE)
                if lines == []:
                    break

                dictionary, docID, postingLists = self.addToDict(dictionary, docID, lines, postingLists)

                self.writeOnDisk(dictionary, currBlockNum, postingLists)
                blocksList.append(currBlockNum)
                currBlockNum += 1
                del dictionary
                del postingLists

        # we have finished reading all the file
        self.numberOfDocs = docID
        # merge all blocks into one large block by multiprocessing
        self.currBlockNum = currBlockNum

        while len(blocksList) > 1:
            i = 0
            while i < len(blocksList) - 1:
                lock = Lock()
                manager1 = Manager()
                manager2 = Manager()
                dict1 = manager1.dict()
                dict2 = manager2.dict()
                list1 = [blocksList.pop(i), blocksList.pop(i)]
                list2 = []
                if i < len(blocksList) - 1 and len(blocksList) > 1:
                    list2 = [blocksList.pop(i), blocksList.pop(i)]

                p1 = Process(target=self.mergeBlocks, args=(list1, dict1, lock))
                p2 = Process(target=self.mergeBlocks, args=(list2, dict2, lock))

                p1.start()
                self.currBlockNum += 1
                if list2 != []:
                    p2.start()
                    self.currBlockNum += 1
                p1.join()
                if list2 != []:
                    p2.join()

                if 'return' in dict1:
                    blocksList.insert(i, dict1['return'])

                if list2 != []:
                    blocksList.insert(i + 1, dict2['return'])

                i += 1

        self.indexOutPutFile = blocksList[0]

        # write number of docs on disc
        self.writeNumberOfDocsOnDisk()
        self.renameFilesNames()

    def renameFilesNames(self):
        """
        method to rename files name after merging all files if needed
        :return:
        """
        blockNum = self.indexOutPutFile
        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME + str(blockNum))
        PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME + str(blockNum))
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME + str(blockNum))
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME + str(blockNum))
        TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME + str(blockNum))
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME + str(blockNum))
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME + str(blockNum))
        NumberOfDocsPath = PATH.format(self.indexDir, DOCS_NUMBER_FILE_NAME + str(blockNum))

        os.rename(DictionaryPath, PATH.format(self.indexDir, DICTIONARY_FILE_NAME))
        os.rename(PostingListsPath, PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME))
        os.rename(FCDataPath, PATH.format(self.indexDir, FC_DATA_FILE_NAME))
        os.rename(DocsFreqPath, PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME))
        os.rename(TermsFreqPath, PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME))
        os.rename(PostingListsPointersPath, PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME))
        os.rename(TermsFreqPointersPath, PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME))
        os.rename(NumberOfDocsPath, PATH.format(self.indexDir, DOCS_NUMBER_FILE_NAME))

    def writeNumberOfDocsOnDisk(self):
        """
        method to write number of docs to the disk
        :return:
        """
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)
        numberOfDocsStream = encodeObj.getEncode([self.numberOfDocs])
        NumberOfDocsPath = PATH.format(self.indexDir, DOCS_NUMBER_FILE_NAME + str(self.indexOutPutFile))

        with open(NumberOfDocsPath, 'ab+') as numberOfDocsFid:
            numberOfDocsFid.write(numberOfDocsStream)

    def deleteFromDisk(self, blockNum):
        """
        method to delete a certain block from disk
        :param blockNum:
        :return: void
        """
        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME + str(blockNum))
        PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME + str(blockNum))
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME + str(blockNum))
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME + str(blockNum))
        TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME + str(blockNum))
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME + str(blockNum))
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME + str(blockNum))

        if os.path.isfile(DictionaryPath):
            os.remove(DictionaryPath)
        if os.path.isfile(PostingListsPath):
            os.remove(PostingListsPath)
        if os.path.isfile(FCDataPath):
            os.remove(FCDataPath)
        if os.path.isfile(DocsFreqPath):
            os.remove(DocsFreqPath)
        if os.path.isfile(TermsFreqPath):
            os.remove(TermsFreqPath)
        if os.path.isfile(PostingListsPointersPath):
            os.remove(PostingListsPointersPath)
        if os.path.isfile(TermsFreqPointersPath):
            os.remove(TermsFreqPointersPath)


    def mergeBlocks(self, blocksList, dict, lock):
        """
        method to merge blocks, it merges two blocks at one time till the number of blocks on
        disk is one in a multiprocessing way

        :param blocksList: list of blocks numbers that has been written on disk
        :param dict: dict of the current process to write in the result block for the main process
        :param lock: lock to use when updating number of block
        :return: void
        """

        lock.acquire()
        currBlockNum = self.currBlockNum
        lock.release()

        while len(blocksList) > 1:
            i = 0
            while i < len(blocksList) - 1:
                block1 = blocksList.pop(i)
                block2 = blocksList.pop(i)
                blocksList.insert(i, currBlockNum)
                dict1 = self.getDictionaryFromFiles(block1)
                dict2 = self.getDictionaryFromFiles(block2)
                dictRes = {}

                postingListsPointers = []
                termsFreqPointers = []

                terms1, docsFreq1 = self.getDetailsFromDict(dict1)
                terms2, docsFreq2 = self.getDetailsFromDict(dict2)

                TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME + str(currBlockNum))
                PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME + str(currBlockNum))

                p1, p2 = 0, 0

                with open(PostingListsPath, 'ab+') as  postingListsFid, open(TermsFreqPath, 'ab+') as termsFreqFid:
                    postingListsPointer, termsFreqpointer = 0, 0
                    while p1 < len(terms1) and p2 < len(terms2):

                        firstTerm = terms1[p1]
                        secondTerm = terms2[p2]

                        currPostingList = []
                        currTermsFreq = []

                        if firstTerm < secondTerm:
                            dictRes[firstTerm] = [dict1[firstTerm][0]]
                            currPostingList, currTermsFreq = self.getSpecifTokenPostingList(block1, firstTerm, dict1, terms1, p1)
                            p1 += 1
                        elif secondTerm < firstTerm:
                            dictRes[secondTerm] = [dict2[secondTerm][0]]
                            currPostingList, currTermsFreq = self.getSpecifTokenPostingList(block2, secondTerm, dict2, terms2, p2)
                            p2 += 1
                        else:
                            dictRes[firstTerm] = [dict1[firstTerm][0] + dict2[secondTerm][0]]
                            firstPostingList, firstTermsFreq = self.getSpecifTokenPostingList(block1, firstTerm, dict1, terms1, p1)
                            secondPosting, secondTerms = self.getSpecifTokenPostingList(block2, secondTerm, dict2, terms2, p2)

                            firstPostingList.extend(secondPosting)
                            firstTermsFreq.extend(secondTerms)

                            currPostingList = firstPostingList
                            currTermsFreq = firstTermsFreq
                            p1 += 1
                            p2 += 1

                        currEncode = PostingList(currPostingList, VARIANT_ENCODE_TYPE)
                        currPostingListStream = currEncode.GetList()
                        currTermFreqStream = currEncode.getEncode(currTermsFreq)

                        postingListsFid.write(currPostingListStream)
                        termsFreqFid.write(currTermFreqStream)
                        postingListsPointers.append(postingListsPointer)
                        termsFreqPointers.append(termsFreqpointer)

                        postingListsPointer += len(currPostingListStream)
                        termsFreqpointer += len(currTermFreqStream)

                    while p1 < len(terms1):

                        firstTerm = terms1[p1]
                        dictRes[firstTerm] = [dict1[firstTerm][0]]

                        currPostingList, currTermsFreq = self.getSpecifTokenPostingList(block1, firstTerm, dict1, terms1, p1)

                        currEncode = PostingList(currPostingList, VARIANT_ENCODE_TYPE)
                        currPostingListStream = currEncode.GetList()
                        currTermFreqStream = currEncode.getEncode(currTermsFreq)

                        postingListsFid.write(currPostingListStream)
                        termsFreqFid.write(currTermFreqStream)

                        postingListsPointers.append(postingListsPointer)
                        termsFreqPointers.append(termsFreqpointer)

                        postingListsPointer += len(currPostingListStream)
                        termsFreqpointer += len(currTermFreqStream)

                        p1 += 1

                    while p2 < len(terms2):

                        secondTerm = terms2[p2]
                        dictRes[secondTerm] = [dict2[secondTerm][0]]

                        currPostingList, currTermsFreq = self.getSpecifTokenPostingList(block2, secondTerm, dict2, terms2, p2)

                        currEncode = PostingList(currPostingList, VARIANT_ENCODE_TYPE)
                        currPostingListStream = currEncode.GetList()
                        currTermFreqStream = currEncode.getEncode(currTermsFreq)

                        postingListsFid.write(currPostingListStream)
                        termsFreqFid.write(currTermFreqStream)

                        postingListsPointers.append(postingListsPointer)
                        termsFreqPointers.append(termsFreqpointer)

                        postingListsPointer += len(currPostingListStream)
                        termsFreqpointer += len(currTermFreqStream)

                        p2 += 1

                self.writeMergeOnDisk(dictRes, currBlockNum, postingListsPointers, termsFreqPointers)
                self.deleteFromDisk(block1)
                self.deleteFromDisk(block2)

                currBlockNum += 1
                i += 1

        lock.acquire()
        self.currBlockNum = currBlockNum
        lock.release()

        dict['return'] = blocksList[0]


    def writeMergeOnDisk(self, dictionary, blockNum, postingListsPointersList, termsFreqPointersList):
        """
        private method to write the result block of merging on disk
        :param dictionary: result dictionary
        :param blockNum: number of the new block
        :param postingListsPointersList: list of pointers to posting lists of the terms
        :param termsFreqPointersList: list of pointers to terms frequencies
        :return:
        """
        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME + str(blockNum))
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME + str(blockNum))
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME + str(blockNum))
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME + str(blockNum))
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME + str(blockNum))

        terms, docsFreq = self.getDetailsFromDict(dictionary)
        FCObj = Dictionary(terms, (COMPRESSION_TYPE, COMPRESSION_BLOCKS))
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)
        dictionaryStream = FCObj.str
        docsFreqStream = encodeObj.getEncode(docsFreq)
        FCData = self.getFCDataFromDict(FCObj.dict)
        FCDataStream = encodeObj.getEncode(FCData)

        with open(DictionaryPath, 'a+') as dictFid, open(FCDataPath, 'ab+') as FCDataFid, open(DocsFreqPath,
                                                                                               'ab+') as docsFreqFid:
            dictFid.write(dictionaryStream)
            FCDataFid.write(FCDataStream)
            docsFreqFid.write(docsFreqStream)

        postingPointersEncode = PostingList(postingListsPointersList, VARIANT_ENCODE_TYPE)
        termsFreqPointersEncode = PostingList(termsFreqPointersList, VARIANT_ENCODE_TYPE)

        postingListsPointersStream = postingPointersEncode.GetList()
        termsFreqPointersStream = termsFreqPointersEncode.GetList()

        with open(PostingListsPointersPath, 'ab+') as postingListsPointersFid, open(TermsFreqPointersPath,
                                                                                    'ab+') as termsFreqPointersFid:
            postingListsPointersFid.write(postingListsPointersStream)
            termsFreqPointersFid.write(termsFreqPointersStream)

    def getSpecifTokenPostingList(self, block, token, dict, terms, idx):
        """
        private method to get a specific term posting lists and terms frequencies
        :param block: number of the block
        :param token: the term
        :param dict: dictionary of terms
        :param terms: list of terms
        :param idx: index of the term in the list
        :return: posting list and terms frequencies
        """
        start = idx
        if idx == len(terms) - 1:
            end = start
        else:
            end = idx + 1

        TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME + str(block))
        PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME + str(block))

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

        postingsRes, termsRes =[], []
        for i in range(dict[token][0]):
            postingsRes.append(postingLists[i])
            termsRes.append(termsFreq[i])


        return postingsRes, termsRes


    def getFCDataFromFile(self, lst):
        """
        private method to get front coding data from list of numbers that we got from file
        in disk
        :param lst:
        :return: dictionary for front coding object
        """
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

    def getDictionaryFromFiles(self, blockNum):
        """
        private method to get the dictionary from the files on the disc
        :param blockNum: the number of the block to get data from
        :return: dictionary of terms
        """
        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME + str(blockNum))
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME + str(blockNum))
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME + str(blockNum))
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME + str(blockNum))
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME + str(blockNum))

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

        for i, term in enumerate(terms):
            dict[term] = [docsFreq[i], -1, postingListsPointers[i], termsFreqPointers[i]]

        return dict

    def getFCDataFromDict(self, dict):
        """
        private method to get data that is essential for front coding compression from the
        dictionary of Front Coding Object
        :param dict: Front Coding Object dictionary
        :return: list of numbers
        """
        lst = []
        for block in dict:
            lst.append(block[0])
            i = 1
            while i < len(block):
                lst.append(block[i][0])
                lst.append(block[i][1])
                i += 1

        return lst

    def getDetailsFromDict(self, dictionary):
        """
        private method to get terms and docs frequency  from dictionary
        :param dictionary: dictionary of terms
        :return: terms, docsFreq
        """
        terms, docsFreq = [], []

        for term in sorted(dictionary):
            terms.append(term)
            docsFreq.append(dictionary[term][0])

        return terms, docsFreq

    def writeOnDisk(self, dictionary, blockNum, postingLists):
        """
        private method to write sorted compressed dictionary and posting lists
        on desk, it will write 7 files for current block:
        first file is Dictionary[blockNum].txt which contains the compressed terms.
        second file is FCData[blockNum].txt which contains the compressed front coding
        data (block start, (term Length, Prefix length) ...).
        third file is DocsFreq[blockNum].txt which contains the documents frequencies.
        fourth file is PostingLists[blockNum].txt which contains the postings lists
        for all terms in dictionary.
        fifth file is TermsFreq[blockNum].txt which contains the terms frequency
        for each term in each posting list.
        sixth file is PostingListsPoitners[blockNum].txt which contains the pointers
        to the posting list for each term in the disc.
        seventh file is TermsFreqPointers[blockNum].txt which contains the pointers
        to the terms freq list for each term in the disc.
        :param dictionary: dictionary of words and docs frequency and pointer to posting lists
        :param blockNum: number of the block to write on the desk
        :param postingLists: list of posting lists for the dictionary
        :return: void
        """
        if not (os.path.isdir(self.indexDir)):
            os.mkdir(self.indexDir)

        DictionaryPath = PATH.format(self.indexDir, DICTIONARY_FILE_NAME + str(blockNum))
        PostingListsPath = PATH.format(self.indexDir, POSTING_LISTS_FILE_NAME + str(blockNum))
        FCDataPath = PATH.format(self.indexDir, FC_DATA_FILE_NAME + str(blockNum))
        DocsFreqPath = PATH.format(self.indexDir, DOCS_FREQ_FiLE_NAME + str(blockNum))
        TermsFreqPath = PATH.format(self.indexDir, TERMS_FREQ_FILE_NAME + str(blockNum))
        PostingListsPointersPath = PATH.format(self.indexDir, POSTING_LISTS_POINTERS_FILE_NAME + str(blockNum))
        TermsFreqPointersPath = PATH.format(self.indexDir, TERMS_FREQ_POINTERS_FILE_NAME + str(blockNum))

        terms, docsFreq = self.getDetailsFromDict(dictionary)
        FCObj = Dictionary(terms, (COMPRESSION_TYPE, COMPRESSION_BLOCKS))
        encodeObj = PostingList([], VARIANT_ENCODE_TYPE)
        dictionaryStream = FCObj.str
        docsFreqStream = encodeObj.getEncode(docsFreq)
        FCData = self.getFCDataFromDict(FCObj.dict)
        FCDataStream = encodeObj.getEncode(FCData)
        postingListsStream = bytearray()
        termsFreqStream = bytearray()

        with open(DictionaryPath, 'a+') as dictFid, open(FCDataPath, 'ab+') as FCDataFid, open(DocsFreqPath,
                                                                                               'ab+') as docsFreqFid:
            dictFid.write(dictionaryStream)
            FCDataFid.write(FCDataStream)
            docsFreqFid.write(docsFreqStream)

        postingListsPointer, termsFreqPointer = 0, 0
        postingListsPointersList, termsFreqPointersList = [], []
        for term in terms:
            currPostingList, currTermFreqList = [], []

            for pair in postingLists[dictionary[term][1]]:
                currPostingList.append(pair[0])
                currTermFreqList.append(pair[1])

            currEncode = PostingList(currPostingList, VARIANT_ENCODE_TYPE)
            currPostingListStream = currEncode.GetList()
            postingListsStream.extend(currPostingListStream)

            currTermFreqStream = currEncode.getEncode(currTermFreqList)
            termsFreqStream.extend(currTermFreqStream)

            postingListsPointersList.append(postingListsPointer)
            termsFreqPointersList.append(termsFreqPointer)

            postingListsPointer += len(currPostingListStream)
            termsFreqPointer += len(currTermFreqStream)

        postingPointersEncode = PostingList(postingListsPointersList, VARIANT_ENCODE_TYPE)
        termsFreqPointersEncode = PostingList(termsFreqPointersList, VARIANT_ENCODE_TYPE)

        postingListsPointersStream = postingPointersEncode.GetList()
        termsFreqPointersStream = termsFreqPointersEncode.GetList()

        with open(PostingListsPath, 'ab+') as  postingListsFid, open(TermsFreqPath, 'ab+') as termsFreqFid:
            postingListsFid.write(postingListsStream)
            termsFreqFid.write(termsFreqStream)

        with open(PostingListsPointersPath, 'ab+') as postingListsPointersFid, open(TermsFreqPointersPath,
                                                                                    'ab+') as termsFreqPointersFid:
            postingListsPointersFid.write(postingListsPointersStream)
            termsFreqPointersFid.write(termsFreqPointersStream)

    def addToDict(self, dictionary, docID, lines, postingLists):
        """
        private method to add the terms in lines to the dictionary
        :param dictionary: dictionary of terms
        :param docID: current document ID
        :param lines: array of lines from the input file
        :param postingLists: list of posting lists for the dictionary
        :return: dictionary, docID, postingLists
        """

        for line in lines:
            if line == SPLIT_SIGN:
                docID += 1
                continue
            terms = getMatches(line)
            for term in terms:
                if not (term in dictionary):
                    postingLists.append([[docID, 1]])
                    dictionary[term] = [1, len(postingLists) - 1]
                else:
                    isExist, postingLists[dictionary[term][1]] = self.checkIfDocIdExistAndAdd(docID, postingLists[
                        dictionary[term][1]])
                    if not (isExist):
                        postingLists[dictionary[term][1]].append([docID, 1])
                        dictionary[term][0] += 1

        return dictionary, docID, postingLists

    def checkIfDocIdExistAndAdd(self, docID, postingList):
        """
        private method to check if a certain doc ID is existed in the term posting
        list, if its then it add one to term freq, else it returns false and don't
        change the posting list
        :param docID: document id
        :param postingList: posting list of the specific term
        :return: true or false with the posting list
        """

        if docID == postingList[len(postingList) - 1][0]:
            postingList[len(postingList) - 1][1] += 1
            return True, postingList
        return False, postingList

    def removeIndex(self, dir):
        """Delete all index files by removing the given directory
        dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""
        if os.path.isdir(dir):
            shutil.rmtree(dir)
