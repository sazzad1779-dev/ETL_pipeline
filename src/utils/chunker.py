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
            return []
        
        # Ensure texts is a list of strings
        if isinstance(texts, str):
            texts = [texts]
        
        split_texts = []
        for text in texts:
            split_texts.extend(self.text_splitter.split_text(text))
        
        return split_texts

