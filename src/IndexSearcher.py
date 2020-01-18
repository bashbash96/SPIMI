import math
from IndexReader import IndexReader


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


class IndexSearcher:
    def __init__(self, iReader):
        """Constructor.
        iReader is the IndexReader object on which the search should be performed """
        self.indexReader = iReader
        self.numOfDocs = iReader.getNumberOfDocuments()
        self.dictionary = {}
        self.postingLists = []
        self.pairsPostingLists = []

    def vectorSpaceSearch(self, query, k):
        """Returns a tupple containing the id-s of the k most highly ranked reviews
         for the given query, using the vector space ranking function lnn.ltc
         (using the SMART notation). The id-s should be sorted by the ranking."""

        listOfTerms = getMatches(query)

        if listOfTerms == []:
            return ()

        dictionary = {}
        queryDict = self.getQueryTF(listOfTerms)

        for term in listOfTerms:
            if not (term in dictionary):
                dictionary[term] = self.indexReader.getTokenFrequency(term)

        sortedListOfterms = sorted(dictionary.items(), key=lambda val: val[1])

        listOfCommonDcIDs = self.getIDsIntersect(sortedListOfterms)

        if listOfCommonDcIDs == []:
            return ()

        queryDict = self.calculateQueryWT(queryDict)

        scores = self.calculateScores(listOfCommonDcIDs, queryDict)

        i = 0
        res = ()
        while i < k and i < len(scores):
            res = res + (scores[i][0],)
            i += 1

        return res

    def calculateScores(self, listOfIDs, queryDict):
        res = []
        for id in listOfIDs:
            score = 0
            for term in queryDict:
                wtID = self.getTFInID(id, term)
                score += wtID * queryDict[term][3]
            curr = (id, round(score, 8))
            res.append(curr)

        res = sorted(res, reverse=True, key=lambda val: val[1])

        return res

    def calculateQueryWT(self, dict):
        res = {}

        for term in dict:
            if not (term in res):
                tf = 1 + math.log(dict[term][0], 10)
                idf = self.getIDF(term)
                wt = tf * idf
                res[term] = [tf, idf, wt]
        normalize = 0
        for term in res:
            normalize += (res[term][2] ** 2)
        normalize = (normalize ** 0.5)

        for term in res:
            res[term].append(res[term][2] / normalize)

        return res

    def getQueryTF(self, list):
        dict = {}

        for term in list:
            if term in dict:
                dict[term] = [dict[term][0] + 1]
            else:
                dict[term] = [1]

        return dict

    def getIDsIntersect(self, list):

        if not (list[0][0] in self.dictionary):
            res, tf, pairs = self.getListOfDocIDs(list[0][0])
            self.dictionary[list[0][0]] = [list[0][1], len(self.postingLists), len(self.pairsPostingLists), tf]
            self.postingLists.append(res)
            self.pairsPostingLists.append(pairs)
        else:
            res = self.postingLists[self.dictionary[list[0][0]][1]]

        for i in range(1, len(list)):
            if not (list[i][0] in self.dictionary):
                currIDs, tf, pairs = self.getListOfDocIDs(list[i][0])
                self.dictionary[list[i][0]] = [list[i][1], len(self.postingLists), len(self.pairsPostingLists), tf]
                self.postingLists.append(currIDs)
                self.pairsPostingLists.append(pairs)
            else:
                currIDs = self.postingLists[self.dictionary[list[i][0]][1]]

            temp = self.getIntersect(res, currIDs)
            res = temp

            if res == []:
                return res

        return res

    def getIntersect(self, list1, list2):
        res = []
        i = j = 0

        while i < len(list1) and j < len(list2):
            if list1[i] < list2[j]:
                i += 1
            elif list1[i] > list2[j]:
                j += 1
            else:
                res.append(list1[i])
                i += 1
                j += 1

        return res

    def getTermFreqFromPairsList(self, id, list):
        start, end = 0, len(list)

        while start <= end:
            mid = (start + end) // 2
            if id > list[mid][0]:
                start = mid + 1
            elif id < list[mid][0]:
                end = mid - 1
            else:
                return list[mid][1]

        return 0

    def getListOfDocIDs(self, token):
        pairs = self.indexReader.getDocsWithToken(token)
        IDs = []
        sum = 0
        for pair in pairs:
            IDs.append(pair[0])
            sum += pair[1]

        return IDs, sum, pairs

    def getTFInID(self, id, term):
        freq = self.getTermFreqFromPairsList(id, self.pairsPostingLists[self.dictionary[term][2]])
        if freq <= 0:
            return 0
        return 1 + math.log(freq, 10)

    def getIDF(self, term):

        return math.log(self.numOfDocs / self.dictionary[term][0], 10)

