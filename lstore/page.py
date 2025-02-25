import os
from lstore import config
# PHYSICAL PAGE CLASS
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(config.ARRAY_SIZE)

    def has_capacity(self):
        if self.num_records < int(config.ARRAY_SIZE/config.VALUE_SIZE):
            # print(f"{self.num_records}/{int(config.ARRAY_SIZE/config.VALUE_SIZE)}")
            return True
        # print(f"page has no capacity: {self.num_records}/{int(config.ARRAY_SIZE/config.VALUE_SIZE)}")
        return False

    def write(self, value, record_number):
        # assert self.has_capacity()
        assert self.has_capacity()
        if self.has_capacity():
            offset_number = record_number * config.VALUE_SIZE
            self.data[offset_number:offset_number + config.VALUE_SIZE] = value.to_bytes(config.VALUE_SIZE, byteorder='little')
            self.num_records += 1
            # print(f"successfully wrote to page, new number is {self.num_records}")
            return True
        else:
            print(f"failed to write: full, {self.num_records}/{int(config.ARRAY_SIZE/config.VALUE_SIZE)}")
            return False


    def read(self, record_number):
        if record_number >= config.ARRAY_SIZE/config.VALUE_SIZE:
            print("error: invalid index")
            return None
        offset = record_number * config.VALUE_SIZE
        return int.from_bytes(self.data[offset:offset + config.VALUE_SIZE], byteorder='little')

# BASE AND TAIL PAGE CLASS
class PageGroup():
    def __init__(self, num_columns, type=config.TAIL_PAGE):
        self.pages = []
        self.type = type
        self.latest_record_number = 0
        # Initialize pages for each column upfront
        for _ in range(num_columns+4):
            self.pages.append(Page())

    def has_capacity(self):
        # If there are no pages, we assume capacity for a new page
        if not self.pages:
            return True
        elif self.pages[0].has_capacity():
            return True
        else: return False

    def write(self, *record, record_number):
        if not self.has_capacity():
            print("error: No space in page group")
            return False
        # Ensure the number of pages matches the number of columns
        for page, value in zip(self.pages, record):
            page.write(value, record_number)
        return True

# PAGE RANGE CLASS
class pageRange():
    def __init__(self, num_columns):
        # contains an array of PageGroup (base pages)
        self.base_pages = []
        # contains an array of PageGroup (tail pages)
        self.tail_pages = []

        for i in range(config.PAGE_RANGE_SIZE):
            self.base_pages.append(PageGroup(num_columns, type=config.BASE_PAGE))

        self.latest_base_page = 0
        self.latest_tail_page = None
        
    
    def has_capacity(self):
        if self.base_pages[self.latest_base_page].has_capacity():
            return True
        elif self.latest_base_page==15:
            return False
        else:
            self.latest_base_page+=1
            return True


    