from lstore import config
from lstore.db import Database
# BUFFERPOOL CLASS
class Bufferpool:

    def __init__(self, database):
        self.size = 0
        self.database:Database = database
        self.pageGroups = []
        for i in range(config.BUFFERPOOL_MAX_LENGTH):
            self.pageGroups.append(None)

    def hasCapacity(self):
        if self.size<config.BUFFERPOOL_MAX_LENGTH:
            return True
        else: return False

    # if we use this function, then we need to know the RID and record num from the page dir
    def getBufferpoolPage(self, RID, record_num):
        for pageGroup in self.database.bufferpool:
            if RID == pageGroup.pages[config.RID_COLUMN].read(record_num): #idk if this is correct
                pageGroup.pinned = True
                pageGroup.pins+=1
                return pageGroup
        
        # if you make it this far, then the page is not in bufferpool
    
        # check if bufferpool is full
        if self.hasCapacity==False:
            self.purge()

        # read the page from disk
        pageGroup = self.readFromDisk(RID, record_num)
        self.add(pageGroup)
        return pageGroup

    def purge(self):
        leastPinnedIndex = 0
        leastPinnedPins = self.database.bufferpool[0].pins
        for i in range(0, self.database.bufferpool):
            if leastPinnedPins > self.database.bufferpool[i].pins:
                leastPinnedIndex = i
    
        self.evict(leastPinnedIndex)
        return True
    
    def evict(self, index):
        if self.database.bufferpool[index].dirty:
            self.writeToDisk(index) # might be bufferpool[index]
        
        self.database.bufferpool.remove(index)

    def add(self, pageGroup):
        if self.hasCapacity:
            for group in self.pageGroups:
                if group == None:
                    group = pageGroup
                    self.size += 1
                    return True
        else: return False
    
    def remove(self, index):
        self.pageGroups[index] = None
        self.size -= 1
        return True
    
    def readFromDisk(self, RID, record_num):
        # uses rid_column from config and RID to find the page to read
        # if RID == pageGroup.pages[config.RID_COLUMN].read(record_num): #idk if this is correct

        # should work similar to select, maybe always read the things required for index first then just use the index to find what u need
        pass
    
    def writeToDisk(self, index):
        # uses bufferpool[index]
        pass



    