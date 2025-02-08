def test_page_write_read():
    from lstore.page import Page

    page = Page()
    
    # Write value to the first slot
    page.write(42, 0)
    
    # Read value back
    value = page.read(0)
    
    assert value == 42, "Page read/write mismatch"

def test_page_capacity():
    from lstore.page import Page

    page = Page()
    
    # Fill up the page
    for i in range(int(4096/8)):  # 4096 bytes total, 8 bytes per value
        page.write(i, i)
    
    # Should return False since the page is full
    assert page.has_capacity() == False, "Page capacity check failed"