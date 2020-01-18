# --------------- Exercise 1 Solution ------------------
# --------------- Amjad Bashiti - 315899013 ------------


# a func to join list members next to each other
def joinList(lst):
    return ''.join(lst)


# a func to get the prefix size of two words
def getPrefixSize(word1, word2):
    i, size = 0, 0
    while i < len(word1) and i < len(word2):
        if word1[i] == word2[i]:
            size += 1
            i += 1
        else:
            break
    return size


class Dictionary:
    def __init__(self, TermList, type):
        """Given sorted list of terms,
        creates a data structure which holds
        a compressed dictionary
        :param TermList: is the sorted list of terms
        :param type: is one of the following tuples:
        ("STR"), ("BLK", k), ("FC", k) where k is the
        size of the block"""
        self.dict = []
        self.TermList = TermList
        self.type = type
        self.str = self.GetString()
        self.createDict()

    def GetString(self):
        """Returns the dictionary's string"""
        if self.type == 'STR' or self.type[0] == 'BLK':
            return joinList(self.TermList)
        elif self.type[0] == 'FC':
            return self.getFCStr()
        else:
            return -1

    def GetInfo(self, term):
        """ Returns relevant data about term.
        For "STR" it returns the location of the term
        in the string
        For "BLK" it returns a tuple containing the location
        of the container block and the lengths of its terms
        For "FC" it returns a tuple containing the location
        of the container block and pairs containing the lengths
        and prefixes sizes of its terms
        :param term: the term to get info about
        :return:
        """
        if self.type == 'STR':
            return self.getStrInfo(term)
        elif self.type[0] == 'BLK':
            return self.getBLKInfo(term)
        elif self.type[0] == 'FC':
            return self.getFCInfo(term)
        else:
            return -1

    def getStrInfo(self, term):
        first, last, currWord = 0, len(self.dict) - 1, ''
        while first <= last:
            mid = (first + last) // 2
            if mid == len(self.dict) - 1:
                currWord = self.str[self.dict[mid]:]
            else:
                currWord = self.str[self.dict[mid]:self.dict[mid + 1]]
            if term > currWord:
                first = mid + 1
            elif term < currWord:
                last = mid - 1
            else:
                return self.dict[mid]
        return -1

    def getBLKInfo(self, term):
        first, last, firstWord, lastWord = 0, len(self.dict) - 1, '', ''
        while first <= last:
            mid = (first + last) // 2
            firstWord = self.str[self.dict[mid][0]: self.dict[mid][0] + self.dict[mid][1]]
            lastIdx, lastSize = 0, self.dict[mid][len(self.dict[mid]) - 1]
            for i, val in enumerate(self.dict[mid]):
                if i < len(self.dict[mid]) - 1:
                    lastIdx += val
            lastWord = self.str[lastIdx: lastIdx + lastSize]
            if term > lastWord:
                first = mid + 1
            elif term < firstWord:
                last = mid - 1
            else:
                if self.searchWordInBLK(term, self.dict[mid]):
                    return self.dict[mid]
                else:
                    return -1
        return -1

    def searchWordInBLK(self, term, currTuple):
        currIdx = currTuple[0]
        currSize = currTuple[1]
        i = 1
        while i < len(currTuple):
            currWord = self.str[currIdx:currIdx + currSize]
            if currWord == term:
                return True
            i += 1
            if i < len(currTuple):
                currIdx += currSize
                currSize = currTuple[i]
        return False

    def getFCInfo(self, term):
        first, last, firstWord, lastWord = 0, len(self.dict) - 1, '', ''
        while first <= last:
            mid = (first + last) // 2
            wordsList = self.getListOfWords(self.dict[mid])
            firstWord, lastWord = wordsList[0], wordsList[len(self.dict[mid][1]) - 1]
            if term > lastWord:
                first = mid + 1
            elif term < firstWord:
                last = mid - 1
            else:
                if self.searchWordInFC(term, wordsList):
                    return self.dict[mid]
                else:
                    return -1
        return -1

    def searchWordInFC(self, term, wordsList):
        first, last = 0, len(wordsList) - 1
        while first <= last:
            mid = (first + last) // 2
            currWord = wordsList[mid]
            if term > currWord:
                first = mid + 1
            elif term < currWord:
                last = mid - 1
            else:
                return True
        return False

    def getListOfWords(self, currTuple):
        currIdx = currTuple[0]
        lstOfWords = []
        i = 1
        while i < len(currTuple):
            currWord = ''
            currPrefSize = currTuple[i][1]
            currLen = currTuple[i][0] - currPrefSize
            if currPrefSize != 0 and i - 2 >= 0:
                currWord = lstOfWords[i - 2][0:currPrefSize]
            currWord += self.str[currIdx: currLen + currIdx]
            lstOfWords.append(currWord)
            currIdx += currLen
            i += 1

        return lstOfWords

    def getFCStr(self):
        currBLK = 0
        kSize = self.type[1]
        res = ''

        i = 0
        while i < len(self.TermList):
            term = self.TermList[i]
            if i // kSize == currBLK:
                if i % kSize == 0:
                    res += self.TermList[i]
                else:
                    currPrefixSize = getPrefixSize(self.TermList[i - 1], term)
                    res += term[currPrefixSize:]
            else:
                currBLK += 1
                i -= 1
            i += 1
        return res

    def createDict(self):
        if self.type == 'STR':
            self.createStrDict()
        elif self.type[0] == 'BLK':
            self.createBLKDict()
        else:
            self.createFCDict()

    def createStrDict(self):
        currIdx = 0
        self.dict = []
        for term in self.TermList:
            self.dict.append(currIdx)
            currIdx += len(term)

    def createBLKDict(self):
        currBLK = 0
        kSize = self.type[1]
        currBLKIdx = 0
        currWordsList = []
        self.dict = []

        i = 0
        while i < len(self.TermList):
            term = self.TermList[i]
            if i // kSize == currBLK:
                currWordsList.append(len(term))
            else:
                currBLK += 1
                currTuple = (currBLKIdx,)
                for val in currWordsList:
                    currTuple += val,
                    currBLKIdx += val
                self.dict.append(currTuple)
                currWordsList = []
                i -= 1
            i += 1
        if len(currWordsList) != 0:
            currTuple = (currBLKIdx,)
            for val in currWordsList:
                currTuple += val,
            self.dict.append(currTuple)

    def createFCDict(self):
        currBLK = 0
        kSize = self.type[1]
        currBLKIdx = 0
        currWordsList = []

        i = 0
        while i < len(self.TermList):
            term = self.TermList[i]
            if i // kSize == currBLK:
                if i % kSize == 0:
                    currWordsList.append((len(term), 0))
                else:
                    currPrefixSize = getPrefixSize(self.TermList[i - 1], term)
                    currWordsList.append((len(term), currPrefixSize))
            else:
                currBLK += 1
                currTuple = (currBLKIdx,)
                for tuple in currWordsList:
                    currTuple += (tuple,)
                    currBLKIdx += tuple[0]
                    currBLKIdx -= tuple[1]
                self.dict.append(currTuple)
                currWordsList = []
                i -= 1
            i += 1
        if len(currWordsList) != 0:
            currTuple = (currBLKIdx,)
            for tuple in currWordsList:
                currTuple += (tuple,)
            self.dict.append(currTuple)
