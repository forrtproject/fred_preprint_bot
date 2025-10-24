#!/usr/bin/env python3
"""
Author Group Assignment with Co-authorship Network Analysis

This script assigns authors to treatment and control groups while ensuring that
co-authors (authors who have published papers together) are never split across groups.


Usage:
    python3 scripts/assign_author_groups.py \
        --input data/author_networks.csv \
        --output data/author_networks_groups.csv 

Data Format:
    - authors_networks.csv: a csv file with the following columns:doi,given,family,sequence,ORCID,authenticated.orcid,affiliation.name,name,affiliation1.name,affiliation2.name,affiliation3.name,suffix,affiliation.id.id,affiliation.id.id.type,affiliation.id.asserted.by,X.China.,X.San.Marcos..USA.,X.Grand.Forks..ND..USA.,X.Tucson..AZ..USA.,X.Cambridge..USA.,X.Israel.,X.Finland.,X.Poland.,X.Philadelphia..USA.,X.Seattle..USA.,X.Osijek.,X.USA.,X.El.Paso..USA.,X.Tlaquepaque..Mexico.,X.India.India.,X.India.,X.The.Netherlands.,X.Germany.,X.Austria.,X.Switzerland.,X.England.,affiliation1.id.id,affiliation1.id.id.type,affiliation1.id.asserted.by,affiliation2.id.id,affiliation2.id.id.type,affiliation2.id.asserted.by

Strategies:
    - random: Randomly assign authors to groups
    - Avoiding repeat authors in both groups
    
Contamination Handling:
    - exclude: preprints that has  2 or more authors appearing in both groups are excluded

Report Format:
{
  "summary": {
    "total_preprints_dois": 100,
    "total_unique_authors": 150,
    "treatment_preprints": 50,
    "control_preprints": 45,
    "excluded_preprints": 5,
    "treatment_authors": 80,
    "control_authors": 70,
    "excluded_authors": 10,
    "treatment_ratio": 0.533
  },
  "treatment": [
    {
      "doi": "10.1234/example1",
      "authors": ["Mile lewis", "Jane Smith"]
    },
    {
      "doi": "10.1234/example2",
      "authors": ["John Diwa", "Bob Johnson"]
    }
  ],
  "control": [
    {
      "doi": "10.1234/example3",
      "authors": ["Alice Brown", "Charlie Davis"]
    }
  ],
  "excluded": [
    {
      "doi": "10.1234/example4",
      "authors": ["Author With No Group"]
    }
  ]
}

"""

import argparse
import json
import random
import csv
import pandas as pd
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple


class AuthorCoauthorshipGraph:
    """Build and analyze author co-authorship graph."""
    
    def __init__(self, doi_to_authors: Dict[str, List[str]]):
        """
        Initialize graph from DOI -> authors mapping.
        
        Args:
            doi_to_authors: Dictionary mapping DOI strings to list of author names
        """
        self.doi_to_authors = doi_to_authors
        self.dois = list(doi_to_authors.keys())
        self.author_graph = defaultdict(set)  # author -> set of co-authors
        self.author_to_dois = defaultdict(set)  # author -> set of DOIs
        
        self._build_graph()
    
    def _normalize_author(self, name: str) -> str:
        """Normalize author name for consistent matching."""
        if not name or pd.isna(name):
            return ""
        return name.strip().lower()
    
    def _build_graph(self):
        """
        Build co-authorship graph from preprints.
        
        Complete Subgraph Construction
        ==========================================
        For each paper (DOI):
          1. Get list of authors
          2. Create edge between EVERY pair of authors
          3. This forms a complete subgraph (clique) for that paper

        """
        for doi, authors in self.doi_to_authors.items():
            # Normalize and filter empty author names
            normalized_authors = [self._normalize_author(a) for a in authors]
            normalized_authors = [a for a in normalized_authors if a]
            
            # Update mapping
            self.doi_to_authors[doi] = normalized_authors
            
            # Build author -> DOIs mapping
            for author in normalized_authors:
                self.author_to_dois[author].add(doi)
            
            # Build co-authorship edges (complete graph among co-authors)
            # This creates edges for all pairs: O(A²) per paper
            for i, author1 in enumerate(normalized_authors):
                for author2 in normalized_authors[i+1:]:
                    self.author_graph[author1].add(author2)
                    self.author_graph[author2].add(author1)
        
        print(f"Built co-authorship graph:")
        print(f"  {len(self.author_to_dois)} unique authors")
        print(f"  {len(self.doi_to_authors)} preprints (DOIs)")
        print(f"  {sum(len(v) for v in self.author_graph.values()) // 2} co-authorship edges")
    
    def find_connected_components(self) -> List[Set[str]]:
        """
        Find connected components in the co-authorship graph.
        
        Breadth-First Search (BFS) for Connected Components
        ==============================================================
        
        Purpose: Group authors who are connected by co-authorship chains
        
        Example Chain: A→B→C→D means A, B, C, D must all be in same group
          A co-authored with B
          B co-authored with C
          C co-authored with D
          Therefore: All 4 are in one connected component
        
        """
        visited = set()
        components = []
        
        # Get all authors (including those with no co-authors)
        all_authors = set(self.author_to_dois.keys())
        
        for author in all_authors:
            if author in visited:
                continue
            
            # BFS to find connected component
            component = set()
            queue = deque([author])
            
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                
                visited.add(current)
                component.add(current)
                
                # Add all co-authors to queue
                for coauthor in self.author_graph.get(current, []):
                    if coauthor not in visited:
                        queue.append(coauthor)
            
            components.append(component)
        
        # Sort by size for better balancing
        components.sort(key=len, reverse=True)
        
        print(f"\nFound {len(components)} connected author components:")
        print(f"  Largest component: {len(components[0])} authors")
        print(f"  Smallest component: {len(components[-1])} authors")
        print(f"  Solo authors (no co-authors): {sum(1 for c in components if len(c) == 1)}")
        
        return components
    
    def assign_components_to_groups(
        self,
        components: List[Set[str]],
        target_ratio: float = 0.5,
        seed: int = 42
    ) -> Tuple[Set[str], Set[str]]:
        """
        Assign connected components to treatment/control groups.
        """
        random.seed(seed)
        
        # Shuffle components for randomness while maintaining balance
        shuffled_components = components.copy()
        random.shuffle(shuffled_components)
        
        treatment_authors = set()
        control_authors = set()
        
        total_authors = sum(len(comp) for comp in components)
        target_treatment_size = int(total_authors * target_ratio)
        
        # Greedy assignment: add to group that's further from target
        for component in shuffled_components:
            component_size = len(component)
            
            # Calculate how far each group is from its target
            treatment_deficit = target_treatment_size - len(treatment_authors)
            control_deficit = (total_authors - target_treatment_size) - len(control_authors)
            
            # Assign to group with larger deficit (needs more authors)
            if treatment_deficit >= control_deficit:
                treatment_authors.update(component)
            else:
                control_authors.update(component)
        
        print(f"\nAssignment result:")
        print(f"  Treatment: {len(treatment_authors)} authors ({len(treatment_authors)/total_authors*100:.1f}%)")
        print(f"  Control: {len(control_authors)} authors ({len(control_authors)/total_authors*100:.1f}%)")
        print(f"  Target ratio: {target_ratio*100:.1f}% treatment")
        
        return treatment_authors, control_authors
    
    def categorize_preprints(
        self,
        treatment_authors: Set[str],
        control_authors: Set[str]
    ) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Categorize preprints based on their authors' group assignments.
        
        - Treatment preprint: ALL authors in treatment group
        - Control preprint: ALL authors in control group
        - Excluded preprint: Authors from BOTH groups (contaminated) OR no authors
        """
        treatment_dois = set()
        control_dois = set()
        excluded_dois = set()
        
        for doi, authors in self.doi_to_authors.items():
            # Exclude preprints with no authors
            if not authors:
                excluded_dois.add(doi)
                continue
            
            authors_in_treatment = sum(1 for a in authors if a in treatment_authors)
            authors_in_control = sum(1 for a in authors if a in control_authors)
            
            if authors_in_treatment > 0 and authors_in_control > 0:
                # Mixed: has authors from both groups (should not happen with component-based assignment)
                excluded_dois.add(doi)
            elif authors_in_treatment > 0:
                # All authors in treatment
                treatment_dois.add(doi)
            elif authors_in_control > 0:
                # All authors in control
                control_dois.add(doi)
            else:
                # No authors assigned (shouldn't happen)
                excluded_dois.add(doi)
        
        print(f"\nPreprint categorization:")
        print(f"  Treatment preprints: {len(treatment_dois)}")
        print(f"  Control preprints: {len(control_dois)}")
        print(f"  Excluded preprints (no authors or mixed groups): {len(excluded_dois)}")
        
        return treatment_dois, control_dois, excluded_dois
    
    def generate_report(
        self,
        treatment_authors: Set[str],
        control_authors: Set[str],
        treatment_dois: Set[str],
        control_dois: Set[str],
        excluded_dois: Set[str]
    ) -> Dict:
        """Generate comprehensive report."""
        
        # Find excluded authors (those whose preprints were excluded)
        excluded_authors = set()
        for doi in excluded_dois:
            excluded_authors.update(self.doi_to_authors[doi])
        
        # Remove excluded authors from main groups
        treatment_authors_final = treatment_authors - excluded_authors
        control_authors_final = control_authors - excluded_authors
        
        # Build treatment preprints list with authors and DOIs
        treatment_preprints_list = []
        for doi in sorted(treatment_dois):
            treatment_preprints_list.append({
                'doi': doi,
                'authors': self.doi_to_authors[doi]
            })
        
        # Build control preprints list with authors and DOIs
        control_preprints_list = []
        for doi in sorted(control_dois):
            control_preprints_list.append({
                'doi': doi,
                'authors': self.doi_to_authors[doi]
            })
        
        # Build excluded preprints list with authors and DOIs
        excluded_preprints_list = []
        for doi in sorted(excluded_dois):
            excluded_preprints_list.append({
                'doi': doi,
                'authors': self.doi_to_authors[doi]
            })
        
        report = {
            'summary': {
                'total_preprints_dois': len(self.doi_to_authors),
                'total_unique_authors': len(self.author_to_dois),
                'treatment_preprints': len(treatment_dois),
                'control_preprints': len(control_dois),
                'excluded_preprints': len(excluded_dois),
                'treatment_authors': len(treatment_authors_final),
                'control_authors': len(control_authors_final),
                'excluded_authors': len(excluded_authors),
                'treatment_ratio': len(treatment_authors_final) / len(self.author_to_dois) if len(self.author_to_dois) > 0 else 0
            },
            'treatment': treatment_preprints_list,
            'control': control_preprints_list,
            'excluded': excluded_preprints_list
        }
        
        return report


def load_csv_data(input_file: str) -> Dict[str, List[str]]:
    """
    Load author data from CSV and create DOI -> authors mapping.
    
    Args:
        input_file: Path to CSV file with columns: doi, given, family, ...
    
    Returns:
        Dictionary mapping DOI to list of author names
    """
    print(f"Loading data from {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"Loaded {len(df)} rows from CSV")
    print(f"Columns: {', '.join(df.columns[:10])}...")
    
    # Create author full name from given and family names
    # Handle missing values
    df['author_name'] = df.apply(
        lambda row: f"{row.get('given', '')} {row.get('family', '')}".strip(),
        axis=1
    )
    
    # Filter out rows with no DOI or no author name
    df = df[df['doi'].notna() & (df['author_name'] != '')]
    
    print(f"After filtering: {len(df)} valid author-DOI pairs")
    
    # Group by DOI to get list of authors per preprint
    doi_to_authors = {}
    for doi, group in df.groupby('doi'):
        authors = group['author_name'].tolist()
        doi_to_authors[doi] = authors
    
    print(f"Found {len(doi_to_authors)} unique DOIs")
    
    return doi_to_authors


def save_csv_output(
    output_file: str,
    doi_to_authors: Dict[str, List[str]],
    treatment_authors: Set[str],
    control_authors: Set[str],
    treatment_dois: Set[str],
    control_dois: Set[str],
    excluded_dois: Set[str]
):
    """
    Save results to CSV file with columns: doi, author_name, group
    
    Args:
        output_file: Path to output CSV file
        doi_to_authors: Mapping of DOI to author names
        treatment_authors: Set of treatment authors
        control_authors: Set of control authors
        treatment_dois: Set of treatment DOIs
        control_dois: Set of control DOIs
        excluded_dois: Set of excluded DOIs
    """
    rows = []
    
    # Process each DOI
    for doi, authors in doi_to_authors.items():
        # Determine DOI group
        if doi in treatment_dois:
            doi_group = 'treatment'
        elif doi in control_dois:
            doi_group = 'control'
        elif doi in excluded_dois:
            doi_group = 'excluded'
        else:
            doi_group = 'unknown'
        
        # Add row for each author
        for author in authors:
            author_normalized = author.strip().lower()
            
            # Determine author group
            if author_normalized in treatment_authors:
                author_group = 'treatment'
            elif author_normalized in control_authors:
                author_group = 'control'
            else:
                author_group = 'excluded'
            
            rows.append({
                'doi': doi,
                'author_name': author,
                'author_group': author_group,
                'doi_group': doi_group
            })
    
    # Create DataFrame and save
    output_df = pd.DataFrame(rows)
    output_df.to_csv(output_file, index=False)
    print(f"\nSaved {len(output_df)} rows to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Assign authors to treatment/control groups with co-author consistency"
    )
    parser.add_argument(
        '--input',
        default='data/author_networks.csv',
        help="Input CSV file with author data (columns: doi, given, family, ...)"
    )
    parser.add_argument(
        '--output',
        default='data/author_networks_groups.csv',
        help="Output CSV file with group assignments"
    )
    parser.add_argument(
        '--report',
        default='data/author_groups_report.json',
        help="Output JSON file with summary report"
    )
    parser.add_argument(
        '--treatment-ratio',
        type=float,
        default=0.5,
        help="Target ratio of authors to assign to treatment (0-1), default 0.5 for 50/50 split"
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help="Random seed for reproducibility"
    )
    
    args = parser.parse_args()
    
    print(f"{'='*80}")
    print(f"Author Group Assignment with Co-author Consistency")
    print(f"{'='*80}")
    print(f"Target treatment ratio: {args.treatment_ratio*100:.0f}%")
    print(f"Random seed: {args.seed}")
    print(f"{'='*80}\n")
    
    # Load data from CSV
    doi_to_authors = load_csv_data(args.input)
    
    # Build co-authorship graph
    graph = AuthorCoauthorshipGraph(doi_to_authors)
    
    # Find connected components (co-author clusters)
    components = graph.find_connected_components()
    
    # Assign components to groups
    treatment_authors, control_authors = graph.assign_components_to_groups(
        components,
        target_ratio=args.treatment_ratio,
        seed=args.seed
    )
    
    # Verify no overlap (should always be true with this approach)
    overlap = treatment_authors & control_authors
    if overlap:
        print(f"\n⚠️  WARNING: Found {len(overlap)} authors in both groups!")
    else:
        print(f"\n✓ Verified: No author appears in both groups")
    
    # Categorize preprints based on author assignments
    treatment_dois, control_dois, excluded_dois = graph.categorize_preprints(
        treatment_authors, control_authors
    )
    
    # Generate report
    report = graph.generate_report(
        treatment_authors, control_authors,
        treatment_dois, control_dois, excluded_dois
    )
    
    # Save report as JSON
    with open(args.report, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\nReport saved to {args.report}")
    
    # Save results to CSV
    save_csv_output(
        args.output,
        doi_to_authors,
        treatment_authors,
        control_authors,
        treatment_dois,
        control_dois,
        excluded_dois
    )
    
    print(f"\n{'='*80}")
    print(f"Final Summary")
    print(f"{'='*80}")
    print(f"Treatment: {report['summary']['treatment_authors']} authors, "
          f"{report['summary']['treatment_preprints']} preprints")
    print(f"Control: {report['summary']['control_authors']} authors, "
          f"{report['summary']['control_preprints']} preprints")
    print(f"Excluded: {report['summary']['excluded_authors']} authors, "
          f"{report['summary']['excluded_preprints']} preprints (mixed groups)")
    print(f"\nActual treatment ratio: {report['summary']['treatment_ratio']*100:.1f}%")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
