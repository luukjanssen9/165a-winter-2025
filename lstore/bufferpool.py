from lstore import config
# FRAME CLASS
class Frame:
    def __init__(self):
        self.empty = True
        self.page = None
        self.curr_pins = 0
        self.total_pins = 0
        self.dirty = False

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
    def getBufferpoolPage(self, RID, column_number, tableindex):
        # check if the page is already in bufferpool
        for frame in self.frames:
            # return the page if it is already in bufferpool

            # do the col and RID match
            if RID in tableindex.indices[column_number].values():
            # if RID == frame.page.read(column_number): # Check to make sure that this is correct
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
        page = self.readFromDisk(RID, column_number, tableindex)
        if self.add(frame)==False:
            print("error, buffer pool has no space despite capacity check passing")
            return False
        # Should we unpin the page here??
        return page

    # Make space for a new page in the bufferpool
    def purge(self):
        # Iterate through the bufferpool to find the page with the least number of pins
        leastPinnedI = 0
        leastPinnedPins = self.frames[0].total_pins
        for i in range(0, self.frames):
            if leastPinnedPins > self.frames[i].total_pins:
                leastPinnedI = i

        # Evict the page with the least number of pins
        self.evict(leastPinnedI)
        return True
    
    # Evict a page from the bufferpool
    def evict(self, i):
        # return false if the frame is pinned
        if self.frames[i].curr_pins != 0:
            return False

        #Write the page to disk if it is dirty
        if self.frames[i].dirty:
            self.writeToDisk(i) # might be bufferpool[i]
        
        self.pageGroups.remove(i)

    # Add a pageGroup to the bufferpool
    def add(self, page):
        if self.hasCapacity:
            # Find and open spot in the bufferpool and add the page
            for frame in self.frames:
                if frame.page == None:
                    frame.page = page
                    self.size += 1
                    return True
                # else: continue
        else: return False
    
    # Remove a page from the bufferpool
    def remove(self, index):
        self.frames[index].page = None
        self.size -= 1
        return True
    
    def readFromDisk(self, RID, column_number, tableindex):
    
        # you have the RID and record_num. use page directory to get the page group and page range. then read them from disk
        # return the page group


        #
        # MAKE SURE YOU CREATE AN INDEX FOR THE PAGE
        #
        # index.indices[column_number] = something like index.create_index()


        # add to the buffer pool with self.add()
        pass
    
    def writeToDisk(self, index):
        # uses bufferpool[index]
        pass



    