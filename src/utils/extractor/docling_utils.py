from docling_core.types.doc.document import DocItemLabel
import yaml
import json
from pathlib import Path
labels_to_include = {
    DocItemLabel.PARAGRAPH,
    DocItemLabel.TEXT,
    DocItemLabel.HANDWRITTEN_TEXT,
    DocItemLabel.CAPTION,
    DocItemLabel.CHART,
    DocItemLabel.DOCUMENT_INDEX,
    DocItemLabel.TITLE
    # DocItemLabel.NUMBERED_LIST,
    # DocItemLabel.IMAGE,  # optional
    # don't include DocItemLabel.TABLE
}

def saving_file(self,res , output_dir,save_format_list=[".md"],table_extraction=False):
    """
    Save the converted document to the specified output directory.
    :param file_path: Path to the input file.
    :param output_dir: Directory where the converted file will be saved.
    """
    stem = res.input.file.stem
    if ".md" in save_format_list:
        md_path = output_dir / f"{stem}.md"
        if table_extraction:
            md_text = res.document.export_to_markdown(labels=labels_to_include)
        else:
            md_text = res.document.export_to_markdown()
        md_path.write_text(md_text, encoding="utf-8")
        self.logger.info(f"   Markdown saved → {md_path}")

    if ".txt" in save_format_list:
        txt_path = output_dir / f"{stem}.txt"
        txt_text = res.document.export_to_text()
        txt_path.write_text(txt_text, encoding="utf-8")
        self.logger.info(f"   Text saved → {txt_path}")

    if ".yaml" in save_format_list:
        yaml_path = output_dir / f"{stem}.yaml"
        yaml_text = yaml.safe_dump(res.document.export_to_dict())
        yaml_path.write_text(yaml_text, encoding="utf-8")
        self.logger.info(f"   YAML saved → {yaml_path}")

    if ".json" in save_format_list:
        json_path = output_dir / f"{stem}.json"
        json_text = json.dumps(res.document.export_to_dict(), ensure_ascii=False, indent=2)
        json_path.write_text(json_text, encoding="utf-8")
        self.logger.info(f"   JSON saved → {json_path}")