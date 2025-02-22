from lstore import config
# FRAME CLASS
class Frame:
    def __init__(self):
        self.empty = True
        self.page = None
        self.pinned = False
        self.pins = 0
        self.dirty = False
        self.pid = None # page id

# BUFFERPOOL CLASS
class Bufferpool:

    def __init__(self, max_size):
        # We need to keep track of the size since the data is stored in a bytearray
        self.size = 0
        self.max_size = max_size
        # Each bufferpool has a list of base and tail pages (initially empty)
        self.frames = []
        for i in range(max_size):
            self.frames.append(None)

    def hasCapacity(self):
        if self.size<self.max_size:
            return True
        else: return False

    # if we use this function, then we need to know the RID and record num from the page dir
    def getBufferpoolPage(self, RID, column_number):
        # check if the page is already in bufferpool
        for frame in self.frames:
            # return the pageGroup if it is already in bufferpool

            # do the col and RID match

                    # pageGroup.pages[config.RID_COLUMN].read(record_num): # Check to make sure that this is correct
            if RID == frame.page.read(record_num): # Check to make sure that this is correct
                frame.pinned = True
                frame.pins+=1
                return frame
        
        #
        # if you make it this far, then the page is not in bufferpool
        #

        # check if bufferpool is full
        if self.hasCapacity==False:
            self.purge()

        # read the page from disk
        page = self.readFromDisk(RID, record_num)
        if self.add(frame)==False:
            print("error, buffer pool has no space despite capacity check passing")
            return False
        # Should we unpin the page here??
        return page

    # Make space for a new page in the bufferpool
    def purge(self):
        # Iterate through the bufferpool to find the page with the least number of pins
        leastPinnedIndex = 0
        leastPinnedPins = self.pageGroups[0].pins
        for i in range(0, self.pageGroups):
            if leastPinnedPins > self.pageGroups[i].pins:
                leastPinnedIndex = i

        # Evict the page with the least number of pins
        self.evict(leastPinnedIndex)
        return True
    
    # Evict a page from the bufferpool
    def evict(self, index):
        #Write the page to disk if it is dirty
        if self.pageGroups[index].dirty:
            self.writeToDisk(index) # might be bufferpool[index]
        
        self.pageGroups.remove(index)

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





        # you have the RID and record_num. use page directory to get the page group and page range. then read them from disk
        # return the page group
        pass
    
    def writeToDisk(self, index):
        # uses bufferpool[index]
        pass



    