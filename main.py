from PyPDF2 import PdfFileReader, PdfFileWriter

def split_pdf(input_file_path, chapter, fromPage, toPage ):
    with open(input_file_path, 'rb') as input_file:
        pdf_reader = PdfFileReader(input_file)
        pdf_writer = PdfFileWriter()
        for page_number in range(pdf_reader.getNumPages()):
            if page_number < fromPage or page_number >= toPage: continue
            pdf_writer.addPage(pdf_reader.getPage(page_number))
        output_file_path = "D:\\浩睿\\棋魂第{}话.pdf".format(chapter)
        with open(output_file_path, 'wb') as output_file:
            pdf_writer.write(output_file)

def split_multi_pdf(input_file_path, startChapter, pageList):
    for index in range(0, len(pageList)-1):
        print(index)
        split_pdf(input_file_path, startChapter+index, pageList[index], pageList[index+1])

if __name__ == "__main__":
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷05.pdf", 35, [1, 27, 47, 69, 93, 115, 137, 157, 179, 203])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷06.pdf", 44, [1,27,49,69,91,113,135,155,179,211])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷07.pdf", 52, [1, 29,49,70,97,117,141,163,187,205])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷08.pdf", 61, [1, 29,51,73,93,113,137,159,183,202, 205])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷09.pdf", 70, [1, 29, 53,74,97,119,141,163,187,207])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷10.pdf", 79, [1, 29,51,71,91,115,137,157,179, 203])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷11.pdf", 88, [1, 27,47,67,87,107,127,153,177,197])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷12.pdf", 97, [1,31, 55,77,101,125,145,169, 190])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷13.pdf", 105, [1, 29,53,75,95,115,137,157,177,195])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷14.pdf", 114, [1, 29,51,73,97,117,141,163,187,189])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷15.pdf", 122, [1, 29,53,79,99,125,147,169,191,211])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷16.pdf", 131, [1, 29,49,71,90,113,133,153,173,191])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷17.pdf", 140, [1, 29,53,73,93,113,137,159,181,209])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷18.pdf", 1, [1, 41,73,105,137,169,211])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷19.pdf", 149, [1, 37,63,83,107,127,151,173,193])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷20.pdf", 157, [1, 27,51,71,95,115,137,157,181,201])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷21.pdf", 166, [1, 27,49,69,89,109,129,149,169,189])
    # split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷22.pdf", 175, [1, 27,49,67,89,109,129,147,169,189])
    split_multi_pdf("D:\\浩睿\\Q-棋魂\\卷23.pdf", 184, [1, 27,47,65,85,105, 137, 164, 190])
