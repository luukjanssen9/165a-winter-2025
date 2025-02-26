from lstore import config
from lstore.page import Page
# FRAME CLASS
class Frame:
    def __init__(self):
        self.empty = True
        self.page = None
        self.path = None
        self.column_number = None
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
            self.frames.append(Frame())

    def hasCapacity(self):
        if self.size<self.max_size:
            return True
        else: return False

    # if we use this function, then we need to know the RID and record num from the page dir
    def getBufferpoolPage(self, RID, column_number, table):

        # check if the page is already in bufferpool:
        for frame in self.frames: # ensure that frame is not empty
            if frame.empty:
                continue

            # return the page if it is already in bufferpool
            for i in range (0,frame.page.num_records): # loop through records in the page
                #read each record in the page
                record = page.read(i)
                # do the col and RID match
                if RID == table.index.indices[column_number][record]: #TODO: check if this is corrects
                # if RID == frame.page.read(column_number): # Check to make sure that this is correct
                    # frame.curr_pins+=1
                    frame.total_pins+=1
                    return frame.page
        
        #
        # if you make it this far, then the page is not in bufferpool
        #

        # check if bufferpool is full
        if self.hasCapacity==False:
            self.purge()

        # read the page from disk
        page = self.readFromDisk(RID, column_number, table)
        if self.add(page, table.path, column_number)==False:
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
    def add(self, page, path, column_number):
        if self.hasCapacity:
            # Find and open spot in the bufferpool and add the page
            for frame in self.frames:
                if frame.empty:
                    frame.empty = False
                    frame.page = page
                    frame.path = path
                    frame.column_number = column_number
                    self.size += 1
                    return True
                # else: continue
        else: return False
    
    # Remove a page from the bufferpool
    def remove(self, index):
        self.frames[index].page = None
        self.frames[index].path = None
        self.frames[index].column_number = None
        self.frames[index].curr_pins = 0
        self.frames[index].total_pins = 0
        self.frames[index].dirty = False
        self.frames[index].empty = True
        self.size -= 1
        return True
    
    def readFromDisk(self, RID, column_number, table):
        # get page group number and basePage number from page directory
        page_range_num, base_page_num, record_num = table.page_directory[RID]

        # TODO: this won't work (base pase number is a number, I thinkwe have to get a page form disk or buferpool)
        # TODO: also, we don't need tail page for insert
        # get the tailpage number from the base page indirection column
        tail_page_rid = base_page_num.pages[config.INDIRECTION_COLUMN].read(record_num) 

        # TODO: get tail page number (is the tail page RID the same as the tail page number? I don't think so) 

        if tail_page_rid == 0:
            # if there is no tail page, then the base page is the tail page
            full_path = f"{table.path}/{page_range_num}/b{base_page_num}/col{column_number}.bin"
            met_path = f"{table.path}/{page_range_num}/b{base_page_num}/col{column_number}.json"
        else:
            # if there is a tail page, then we need to find the tail page
            full_path = f"{table.path}/{page_range_num}/t{tail_page_rid}/col{column_number}.bin"
            met_path = f"{table.path}/{page_range_num}/t{tail_page_rid}/col{column_number}.json"

        # read the page from disk
        with open(full_path, 'rb') as data_file:
            page_data = bytearray(data_file.read())
            # data_file.close()

        # read the metadata from disk
        with open(met_path, 'rb') as metadata_file:
            num_records = int(metadata_file.read())
            # data_file.close()

        page = Page(page_data, num_records)    

        # update the index for this record
        for i in range(page.num_records):
            #read each record in the page
            record = page.read(i)
            # add to index
            table.index.indices[column_number][record] = RID
        return page
        
    
    def writeToDisk(self, bufferpoolIndex):
        # get bufferpool page
        frame = self.frames[bufferpoolIndex]

        full_path = f"{frame.path}/col{frame.column_number}.bin"
        met_path = f"{frame.path}/col{frame.column_number}.json"

        # write the page to disk
        with open(full_path, 'wb+') as data_file:
            data_file.write(frame.page.data)
            # data_file.close()

        # write the metadata to disk
        with open(full_path, 'w+') as metadata_file:
            metadata_file.write(frame.page.num_records)
            # metadata_file.close()



    