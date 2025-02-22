from lstore import config
from lstore.db import Database
# BUFFERPOOL CLASS
class Bufferpool:

    def __init__(self, database):
        # We need to keep track of the size since the data is stored in a bytearray
        self.size = 0
        # One bufferpool per database
        self.database:Database = database
        # Each bufferpool has a list of base and tail pages (initially empty)
        self.pageGroups = []
        for i in range(config.BUFFERPOOL_MAX_LENGTH):
            self.pageGroups.append(None)

    def hasCapacity(self):
        if self.size<config.BUFFERPOOL_MAX_LENGTH:
            return True
        else: return False

    # if we use this function, then we need to know the RID and record num from the page dir
    def getBufferpoolPage(self, RID, record_num):
        # check if the pageGroup is already in bufferpool
        for pageGroup in self.database.bufferpool:
            # return the pageGroup if it is already in bufferpool
            if RID == pageGroup.pages[config.RID_COLUMN].read(record_num): # Check to make sure that this is correct
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

        # Should we unpin the page here??
        return pageGroup

    # Make space for a new page in the bufferpool
    def purge(self):
        # Iterate through the bufferpool to find the page with the least number of pins
        leastPinnedIndex = 0
        leastPinnedPins = self.database.bufferpool[0].pins
        for i in range(0, self.database.bufferpool):
            if leastPinnedPins > self.database.bufferpool[i].pins:
                leastPinnedIndex = i

        # Evict the page with the least number of pins
        self.evict(leastPinnedIndex)
        return True
    
    # Evict a page from the bufferpool
    def evict(self, index):
        #Write the page to disk if it is dirty
        if self.database.bufferpool[index].dirty:
            self.writeToDisk(index) # might be bufferpool[index]
        
        self.database.bufferpool.remove(index)

    # Add a pageGroup to the bufferpool
    def add(self, pageGroup):
        if self.hasCapacity:
            # Find and open spot in the bufferpool and add the pageGroup
            for group in self.pageGroups:
                if group == None:
                    group = pageGroup
                    self.size += 1
                    return True
        else: return False
    
    # Remove a pageGroup from the bufferpool
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



    