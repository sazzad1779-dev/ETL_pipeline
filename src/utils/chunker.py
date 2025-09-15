from langchain_text_splitters import RecursiveCharacterTextSplitter
class TextChunker:
    """Utility class for splitting text into smaller chunks."""
    def __init__(self, chunk_size=400, chunk_overlap=50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )

    def split_texts(self, texts):
        """Splits texts into smaller chunks."""
        if not texts:
            return [], []
        
        # Ensure texts is a list of strings
        if isinstance(texts, str):
            texts = [texts]
        
        split_text_list = []
        chunk_index = []
        for i,text in enumerate(texts):
            split_text_list.extend(self.text_splitter.split_text(text))
            num_chunks = len(self.text_splitter.split_text(text))
            chunk_index.extend([j for j in range(num_chunks)])
            
        
        return split_text_list,chunk_index


