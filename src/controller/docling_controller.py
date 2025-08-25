import time
from pathlib import Path
import pandas as pd
from src.utils.logger import setup_logger
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.document_converter import (
    DocumentConverter,
    PdfFormatOption,
    WordFormatOption,
)
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.pipeline.simple_pipeline import SimplePipeline
from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
from docling_core.transforms.serializer.markdown import MarkdownDocSerializer
from src.utils.extractor.docling_utils import saving_file
from src.utils.chunker import TextChunker
import re

def clean_text(text: str) -> str:
    # Remove <!-- image -->
    text = re.sub(r'<!--\s*image\s*-->', '', text)
    # Remove multiple newlines -> replace with single newline
    text = re.sub(r'\n+', '\n', text).strip()
    return text

class DoclingController:
    def __init__(self):
        """ Initialize the DoclingConverter with optional Weaviate client and StructuredToSQL instance. """
        
        self.logger = setup_logger("etl_app")
        self.doc_converter = self._build_converter()
        self.chunker = TextChunker( chunk_size=1000, chunk_overlap=200)
        
    def _build_converter(self) -> DocumentConverter:
        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = False
        pipeline_options.do_table_structure = True
        pipeline_options.table_structure_options.do_cell_matching = True
        pipeline_options.do_picture_description = False
        pipeline_options.ocr_options.lang = ["ja", "en"]
        pipeline_options.do_formula_enrichment = False
        pipeline_options.force_backend_text = False
        pipeline_options.generate_picture_images = False
        pipeline_options.images_scale = 2
        pipeline_options.do_picture_classification = False
        pipeline_options.do_picture_description
        pipeline_options.accelerator_options = AcceleratorOptions(
            num_threads=8, device=AcceleratorDevice.AUTO
        )

        return DocumentConverter(
            allowed_formats=[
                InputFormat.PDF,
                InputFormat.IMAGE,
                InputFormat.DOCX,
                InputFormat.HTML,
                InputFormat.PPTX,
                InputFormat.ASCIIDOC,
                InputFormat.CSV,
                InputFormat.MD,
                InputFormat.XLSX,
            ],
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    pipeline_cls=StandardPdfPipeline,
                    backend=PyPdfiumDocumentBackend,
                ),
                InputFormat.DOCX: WordFormatOption(
                    pipeline_cls=SimplePipeline,
                ),
            },
        )

    def process(
        self,
        input_paths,
        output_dir=Path("scratch"),
        table_extraction=False,
        save_format_list=[],#,".md", ".txt", ".yaml", ".json"
    ):
        start_time = time.time()
        print("Total file found :",len(input_paths))
        print(input_paths)
        conv_results = self.doc_converter.convert_all(input_paths)
        texts = []
        sources = []
        for i,res in enumerate(conv_results):
            if table_extraction:
                self.table_extraction(res,output_dir)
            self.logger.info(f"✅ Document converted: {res.input.file.name}")
            self.logger.debug(res.document._export_to_indented_text(max_text_len=16))
            serializer = MarkdownDocSerializer(doc=res.document)
            ser_result = serializer.serialize()
            ser_text = clean_text(ser_result.text)
            text_chunks = self.chunker.split_texts(ser_text)
            
            texts.extend(text_chunks)
            sources.extend([input_paths[i]]* len(text_chunks))
            if len(save_format_list) > 0:
                saving_file(
                    res=res,
                    output_dir=output_dir,
                    save_format_list=save_format_list,
                    table_extraction=table_extraction)
            if i<5:
                print("document number: ",i)
                print("Full Text :", ser_text)
                print("\n\nchunk size :", len(text_chunks))
                print("*" * 20)
            # else:
            #     break
        self.logger.info(f"Total time taken: {time.time() - start_time:.2f} seconds")
        return texts, sources

    def table_extraction(self, res, output_dir):
        """
        Extract tables from a document and save them as CSV files.
        """
        stem = res.input.file.stem
        for table_ix, table in enumerate(res.document.tables):
            table_df: pd.DataFrame = table.export_to_dataframe()
            print(f"## Table {table_ix}")
            print(table_df.to_markdown())
            if table_df.shape[0]==0 or table_df.shape[1]==0:
                self.logger.warning(f"Empty table found in {res.input.file.name}, skipping export.")
                continue
            
            # Save the table as csv
            file_name = f"{stem}-table-{table_ix + 1}"
            table_df = self.struct_to_sql._rename_duplicate_columns(table_df)
            self.struct_to_sql._insert_into_sql(table_df, file_name, mode="replace")
            if self.struct_to_sql:
                self.logger.info(f"Table {table_ix + 1} processed and inserted into SQL.")
            else:
                self.logger.warning("No StructuredToSQL instance provided, skipping SQL insertion.")
            # Save the table as csv
            element_csv_filename = output_dir / f"{file_name}.csv"
            self.logger.info(f"Saving CSV table to {element_csv_filename}")
            table_df.to_csv(element_csv_filename)

# Example usage
if __name__ == "__main__":
    docling = DoclingConverter()
    docling.convert_documents(
        input_paths=["example.pdf"],  # Replace with your file paths
        output_dir=Path("scratch"),
        save_markdown=True,
        save_yaml=False,
        save_text=False,
        save_json=False
    )
