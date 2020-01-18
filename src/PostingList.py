# --------------- Exercise 1 Solution ------------------
# --------------- Amjad Bashiti - 315899013 ------------


class PostingList:
    def __init__(self, DocIDs, type):
        """Given sorted list of DocIDs, creates a
        compressed posting list according to the
        compression type.
        The compressed posting list should be stored as
        byte array"""
        self.type = type
        self.DocIDs = DocIDs
        self.listOfGaps = self.getGaps(DocIDs)

    def GetList(self):
        """Returns a byte-array containing the
        compressed posting list"""
        if self.type == 'V' or self.type == 'GV' or self.type == 'LP':
            return self.getEncode(self.listOfGaps)
        else:
            return -1

    def getDocs(self, byteArray):
        """
        return the doc ids of the decoded bytestream
        :return:  list of doc ids
        """
        if self.type == 'V':
            return self.variantDecode(byteArray)

    def getEncode(self, docIDs):
        byteStream = bytearray()
        if self.type != 'GV':
            for ID in docIDs:
                if self.type == 'V':
                    idEncode = self.getVariantEncode(ID)
                else:
                    idEncode = self.getLengthPrecodedEncode(ID)
                byteStream.extend(idEncode)
        else:
            byteStream = self.getGroupVariantEncode(self.listOfGaps)
        return byteStream

    def getGaps(self, IDs):
        res = []
        for i, val in enumerate(IDs):
            if val < 0:
                print("Error! : negative ID")
                exit(0)
            if i == 0:
                res.append(val)
            else:
                res.append(val - IDs[i - 1])
        return res

    def variantDecode(self, byteStream):
        docIDs = []
        currNum = 0
        for i in byteStream:
            if i < 128:
                currNum = currNum * 128 + i
            else:
                currNum = currNum * 128 + (i-128)
                docIDs.append(currNum)
                currNum = 0
        return docIDs

    def getVariantEncode(self, ID):
        byteStream = bytearray()
        while True:
            byteStream.insert(0, ID % 128)
            if ID < 128:
                break
            ID //= 128
        byteStream[len(byteStream) - 1] += 128
        return byteStream

    def getNumOfBytesNeeded(self, ID):
        if ID >= 0 and ID < 64:
            return 0
        elif ID >= 64 and ID < 16384:
            return 64
        elif ID >= 16384 and ID < 4194304:
            return 128
        else:
            return 192

    def getLengthPrecodedEncode(self, ID):
        byteStream = bytearray()
        copy = ID
        counter = 0
        while True:
            byteStream.insert(0, ID % 256)
            counter += 1
            ID //= 256
            if ID < 256:
                break
        numOfBytes = self.getNumOfBytesNeeded(copy)
        if numOfBytes == 64:
            needed = 2
        elif numOfBytes == 128:
            needed = 3
        else:
            needed = 4
        if numOfBytes > 0:
            if counter == needed - 1:
                byteStream.insert(0, numOfBytes + ID)
            else:
                byteStream.insert(0, ID)
                byteStream.insert(0, numOfBytes)

        return byteStream

    def getBytesNeeded(self, ID):
        if ID >= 0 and ID < 256:
            return 0
        elif ID >= 256 and ID < 65536:
            return 64
        elif ID >= 65536 and ID < 16777216:
            return 128
        else:
            return 192

    def getNumOfBytesNeededGroup(self, lstIDs):
        sum, idx, res = 0, 0, bytearray()

        while idx < len(lstIDs):
            curr = self.getBytesNeeded(lstIDs[idx])
            curr = curr >> (2 * idx)
            sum += curr
            idx += 1
        res.insert(0, sum)
        return res

    def getGroupEncode(self, listIDs):
        byteStream, idx = bytearray(), len(listIDs) - 1

        while idx >= 0:
            currID = listIDs[idx]

            while currID > 256:
                byteStream.insert(0, currID % 256)
                currID //= 256
            if currID > 0:
                byteStream.insert(0, currID)
            idx -= 1

        return byteStream

    def getGroupVariantEncode(self, IDs):
        byteStream, currIdx, currListIDs, bytesNeeded, counter = bytearray(), 0, [], bytearray(), 0

        while currIdx < len(IDs):
            while counter < 4 and currIdx < len(IDs):
                currListIDs.append(IDs[currIdx])
                currIdx += 1
                counter += 1
            bytesNeeded = self.getNumOfBytesNeededGroup(currListIDs)
            byteStream.extend(bytesNeeded)
            currGroupEncode = self.getGroupEncode(currListIDs)
            byteStream.extend(currGroupEncode)
            currListIDs = []
            counter = 0
        if len(self.DocIDs) % 4 != 0:
            numToAdd = 4 - len(self.DocIDs) % 4
            for i in range(numToAdd):
                byteStream.append(0)
        return byteStream
