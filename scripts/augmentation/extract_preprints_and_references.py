#!/usr/bin/env python3
"""
extract_preprints_and_references.py

Extract both preprint metadata AND all references from TEI XML files.
Produces a structured JSON with one entry per preprint containing:
  - Main preprint metadata (from teiHeader)
  - List of all references (from div[@type="references"])

Usage:
    python3 scripts/extract_preprints_and_references.py --data-dir data/preprints --output data/preprints_with_references.json --verbose

Output structure:
{
  "file_path": "path/to/file.xml",
  "preprint": {
    "title": "Main article title",
    "doi": "10.xxxx/xxxxx",
    "authors": ["Author 1", "Author 2"],
    "published_date": "YYYY-MM-DD"
  },
  "references": [
    {
      "ref_id": "b0",
      "title": "Reference title",
      "doi": "10.xxxx/yyyyy",
      "authors": ["Ref Author 1"],
      "journal": "Journal Name",
      "year": "2020"
    }
  ],
  "stats": {
    "total_references": 50,
    "references_with_doi": 30,
    "references_without_doi": 20,
    "doi_coverage_percent": 60.0
  }
}
"""

import argparse
import json
import os
import re
from lxml import etree
from typing import Dict, List, Optional


class TEIExtractor:
    """Extract preprint and reference metadata from TEI XML files."""
    
    def __init__(self):
        self.ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
    
    def parse_file(self, file_path: str) -> Dict:
        """
        Parse a single TEI XML file and extract both preprint and reference metadata.
        """
        # Initialize with default preprint structure
        default_preprint = {
            'title': None,
            'doi': None,
            'authors': [],
            'published_date': None,
            'has_title': False,
            'has_doi': False,
            'has_authors': False,
            'has_published_date': False
        }
        
        result = {
            'file_path': file_path,
            'preprint': default_preprint.copy(),
            'references': [],
            'stats': {
                'total_references': 0,
                'references_with_doi': 0,
                'references_without_doi': 0,
                'doi_coverage_percent': 0.0
            }
        }
        
        try:
            tree = etree.parse(file_path)
            
            # Extract main preprint metadata from teiHeader
            preprint_data = self._extract_preprint_metadata(tree)
            if preprint_data:
                result['preprint'] = preprint_data
            
            # Extract all references from the references section
            result['references'] = self._extract_references(tree)
            
            # Calculate statistics
            total = len(result['references'])
            with_doi = sum(1 for ref in result['references'] if ref.get('has_doi'))
            
            result['stats'] = {
                'total_references': total,
                'references_with_doi': with_doi,
                'references_without_doi': total - with_doi,
                'doi_coverage_percent': round((with_doi / total * 100) if total > 0 else 0.0, 2)
            }
            
        except etree.XMLSyntaxError as e:
            print(f"  XML parsing error: {e}")
        except Exception as e:
            print(f"  Unexpected error: {e}")
        
        return result
    
    def _extract_preprint_metadata(self, tree) -> Dict:
        """Extract the main preprint document metadata from teiHeader."""
        preprint = {
            'title': None,
            'doi': None,
            'authors': [],
            'published_date': None,
            'has_title': False,
            'has_doi': False,
            'has_authors': False,
            'has_published_date': False
        }
        
        try:
            # Extract title from teiHeader > fileDesc > titleStmt
            titles = tree.xpath(
                '//tei:teiHeader//tei:titleStmt/tei:title[@type="main"]/text()',
                namespaces=self.ns
            )
            if titles:
                preprint['title'] = titles[0].strip()
                preprint['has_title'] = True
            
            # Extract DOI from teiHeader (main preprint DOI, not references)
            # Usually in sourceDesc > biblStruct > analytic > idno
            dois = tree.xpath(
                '//tei:teiHeader//tei:sourceDesc//tei:analytic//tei:idno[@type="DOI"]/text()',
                namespaces=self.ns
            )
            if dois:
                for doi in dois:
                    if doi.strip().startswith("10."):
                        preprint['doi'] = doi.strip()
                        preprint['has_doi'] = True
                        break
                if not preprint['has_doi'] and dois:
                    preprint['doi'] = dois[0].strip()
                    preprint['has_doi'] = True
            
            # Extract authors from teiHeader
            author_elements = tree.xpath(
                '//tei:teiHeader//tei:sourceDesc//tei:analytic/tei:author/tei:persName',
                namespaces=self.ns
            )
            if not author_elements:
                # Fallback to titleStmt authors
                author_elements = tree.xpath(
                    '//tei:teiHeader//tei:titleStmt/tei:author/tei:persName',
                    namespaces=self.ns
                )
            
            for author in author_elements:
                try:
                    surname = author.xpath('.//tei:surname/text()', namespaces=self.ns)
                    forename = author.xpath('.//tei:forename/text()', namespaces=self.ns)
                    
                    if surname:
                        surname_text = surname[0].strip()
                        forename_text = forename[0].strip() if forename else ""
                        if forename_text:
                            name = f"{forename_text} {surname_text}"
                        else:
                            name = surname_text
                        preprint['authors'].append(name)
                except Exception:
                    continue
            
            preprint['has_authors'] = bool(preprint['authors'])
            
            # Extract published date
            dates = tree.xpath(
                '//tei:teiHeader//tei:publicationStmt//tei:date[@type="published"]/@when | '
                '//tei:teiHeader//tei:publicationStmt//tei:date[@type="published"]/text()',
                namespaces=self.ns
            )
            if dates:
                preprint['published_date'] = dates[0].strip()
                preprint['has_published_date'] = True
        
        except Exception as e:
            print(f"  Error extracting preprint metadata: {e}")
        
        return preprint
    
    def _extract_references(self, tree) -> List[Dict]:
        """Extract all references from the div[@type='references'] section."""
        references = []
        
        try:
            # Find all biblStruct elements in references section
            biblstructs = tree.xpath(
                '//tei:div[@type="references"]//tei:biblStruct',
                namespaces=self.ns
            )
            
            for bibl in biblstructs:
                ref_data = self._extract_reference_data(bibl)
                if ref_data:
                    references.append(ref_data)
        
        except Exception as e:
            print(f"  Error extracting references: {e}")
        
        return references
    
    def _extract_reference_data(self, biblstruct) -> Optional[Dict]:
        """Extract metadata from a single biblStruct reference element."""
        try:
            ref = {
                'ref_id': None,
                'title': None,
                'authors': [],
                'journal': None,
                'year': None,
                'doi': None,
                'has_doi': False,
                'has_title': False,
                'has_authors': False,
                'has_journal': False,
                'has_year': False
            }
            
            # Extract ref ID
            ref['ref_id'] = biblstruct.get('{http://www.w3.org/XML/1998/namespace}id')
            
            # Extract title (prefer analytic, fall back to monogr)
            titles = biblstruct.xpath(
                './/tei:analytic/tei:title[@level="a"][@type="main"]/text() | '
                './/tei:monogr/tei:title[@level="m"]/text()',
                namespaces=self.ns
            )
            if titles:
                ref['title'] = titles[0].strip()
                ref['has_title'] = True
            
            # Extract DOI
            dois = biblstruct.xpath('.//tei:idno[@type="DOI"]/text()', namespaces=self.ns)
            if dois:
                for doi in dois:
                    if doi.strip().startswith("10."):
                        ref['doi'] = doi.strip()
                        ref['has_doi'] = True
                        break
                if not ref['has_doi'] and dois:
                    ref['doi'] = dois[0].strip()
                    ref['has_doi'] = True
            
            # Extract authors
            author_elements = biblstruct.xpath(
                './/tei:analytic/tei:author/tei:persName | '
                './/tei:monogr/tei:author/tei:persName',
                namespaces=self.ns
            )
            
            for author in author_elements:
                try:
                    surname = author.xpath('.//tei:surname/text()', namespaces=self.ns)
                    forename = author.xpath('.//tei:forename/text()', namespaces=self.ns)
                    
                    if surname:
                        surname_text = surname[0].strip()
                        forename_text = forename[0].strip() if forename else ""
                        if forename_text:
                            name = f"{forename_text} {surname_text}"
                        else:
                            name = surname_text
                        ref['authors'].append(name)
                except Exception:
                    continue
            
            ref['has_authors'] = bool(ref['authors'])
            
            # Extract journal
            journals = biblstruct.xpath('.//tei:monogr/tei:title[@level="j"]/text()', namespaces=self.ns)
            if journals:
                ref['journal'] = journals[0].strip()
                ref['has_journal'] = True
            
            # Extract year
            dates = biblstruct.xpath(
                './/tei:date[@type="published"]/@when',
                namespaces=self.ns
            )
            if dates:
                year_match = re.search(r'(\d{4})', dates[0])
                if year_match:
                    ref['year'] = year_match.group(1)
                    ref['has_year'] = True
            
            if not ref['has_year']:
                # Fall back to text content
                dates = biblstruct.xpath(
                    './/tei:date[@type="published"]/text() | .//tei:date/text()',
                    namespaces=self.ns
                )
                for date_text in dates:
                    year_match = re.search(r'(\d{4})', date_text)
                    if year_match:
                        ref['year'] = year_match.group(1)
                        ref['has_year'] = True
                        break
            
            return ref
        
        except Exception as e:
            # Return None if extraction fails completely
            return None


def scan_directory(data_dir: str, verbose: bool = False) -> List[Dict]:
    """Recursively scan directory for TEI XML files and extract data."""
    extractor = TEIExtractor()
    results = []
    
    xml_files = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    total_files = len(xml_files)
    
    for i, file_path in enumerate(xml_files, 1):
        if verbose:
            print(f"[{i}/{total_files}] Processing {file_path}")
        
        result = extractor.parse_file(file_path)
        results.append(result)
        
        if verbose and result['stats']['total_references'] > 0:
            title = 'No title'
            if result.get('preprint') and result['preprint'].get('title'):
                title = result['preprint']['title'][:60]
            print(f"  Preprint: {title}")
            print(f"  References: {result['stats']['total_references']} "
                  f"({result['stats']['references_with_doi']} with DOI, "
                  f"{result['stats']['doi_coverage_percent']}% coverage)")
    
    return results


def print_summary(results: List[Dict]):
    """Print a summary of extraction results."""
    total_preprints = len(results)
    total_references = sum(r['stats']['total_references'] for r in results)
    total_refs_with_doi = sum(r['stats']['references_with_doi'] for r in results)
    total_refs_without_doi = sum(r['stats']['references_without_doi'] for r in results)
    
    preprints_with_doi = sum(1 for r in results if r['preprint'].get('has_doi'))
    preprints_with_title = sum(1 for r in results if r['preprint'].get('has_title'))
    
    print(f"\n{'='*80}")
    print(f"Extraction Summary")
    print(f"{'='*80}")
    print(f"\nPreprints:")
    print(f"  Total preprints: {total_preprints}")
    print(f"  With title: {preprints_with_title}")
    print(f"  With DOI: {preprints_with_doi}")
    
    print(f"\nReferences:")
    print(f"  Total references: {total_references}")
    print(f"  With DOI: {total_refs_with_doi} ({total_refs_with_doi/total_references*100:.1f}%)")
    print(f"  Without DOI: {total_refs_without_doi} ({total_refs_without_doi/total_references*100:.1f}%)")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Extract preprint metadata and all references from TEI XML files"
    )
    parser.add_argument(
        '--data-dir',
        default='data/preprints',
        help="Directory containing TEI XML files"
    )
    parser.add_argument(
        '--output',
        default='data/preprints_with_references.json',
        help="Output JSON file"
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help="Print detailed processing information"
    )
    
    args = parser.parse_args()
    
    print(f"Scanning {args.data_dir} for TEI XML files...")
    results = scan_directory(args.data_dir, args.verbose)
    
    # Write results to JSON file
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"Results written to {args.output}")
    
    if args.verbose:
        print_summary(results)


if __name__ == "__main__":
    main()
