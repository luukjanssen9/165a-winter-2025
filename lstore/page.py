ARRAY_SIZE = 4096
VALUE_SIZE = 8
PAGE_RANGE_SIZE = 16

# PHYSICAL PAGE CLASS
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(ARRAY_SIZE)

    def has_capacity(self):
        if self.num_records < ARRAY_SIZE/VALUE_SIZE:
            return True
        return False

    def write(self, value, record_number):
        if self.has_capacity():
            offset_number = record_number * VALUE_SIZE
            self.data[offset_number:offset_number + VALUE_SIZE] = value.to_bytes(VALUE_SIZE, byteorder='little')
            self.num_records += 1
            return True
        else:
            print("error: page is full")  
            return False


    def read(self, index):
        if index >= self.num_records:
            print("error: invalid index")
            return None
        offset = index * VALUE_SIZE
        return int.from_bytes(self.data[offset:offset + VALUE_SIZE], byteorder='little')

# BASE AND TAIL PAGE CLASS
class PageGroup():
    def __init__(self, num_columns):
        self.pages = []
        # Initialize pages for each column upfront
        for _ in range(num_columns+4):
            self.pages.append(Page())

    def has_capacity(self):
        # If there are no pages, we assume capacity for a new page
        if not self.pages:
            return True
        
        for page in self.pages:
            if page.has_capacity():
                return True
        return False

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
        # contains an array of PageGroup (tail pages)
        self.base_pages = []
        self.tail_pages = []
        for i in range(PAGE_RANGE_SIZE):
            self.base_pages.append(PageGroup(num_columns))
    
    def has_capacity(self):
        for base_page in self.base_pages:
            if base_page.has_capacity():
                return True
        return False


    