"""
Visualize Author Collaboration Networks with Treatment/Control Groups

This script creates network visualizations showing treatment/control group assignments,
collaboration patterns, and cluster statistics.

   
Usage:
    # Basic visualization with stats
    python3 scripts/visualize_author_network.py \
        --input data/author_networks_groups.csv \
        --output author_network.png

    # Show only top 150 most connected authors
    python3 scripts/visualize_author_network.py \
        --input data/author_networks_groups.csv \
        --output author_network.png \
        --top-n 150

    # Highlight cluster boundaries with shaded regions
    python3 scripts/visualize_author_network.py \
        --input data/author_networks_groups.csv \
        --output author_network.png \
        --show-clusters
"""

import argparse
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend


def load_author_network_from_csv(input_file: str) -> Tuple[Dict[str, Set[str]], Dict[str, str]]:
    """
    Load author collaboration network from the output CSV of assign_author_groups.py
    
    """
    print(f"Loading author network from {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {', '.join(df.columns)}")
    
    # Normalize author names
    df['author_normalized'] = df['author_name'].str.strip().str.lower()
    
    # Build DOI -> authors mapping
    doi_to_authors = defaultdict(list)
    for _, row in df.iterrows():
        doi = row['doi']
        author = row['author_normalized']
        doi_to_authors[doi].append(author)
    
    # Build author -> co-authors graph
    author_graph = defaultdict(set)
    for doi, authors in doi_to_authors.items():
        # Create edges between all pairs of co-authors
        for i, author1 in enumerate(authors):
            for author2 in authors[i+1:]:
                author_graph[author1].add(author2)
                author_graph[author2].add(author1)
    
    # Build author -> group mapping
    author_groups = {}
    for _, row in df.iterrows():
        author = row['author_normalized']
        group = row['author_group']
        author_groups[author] = group
    
    print(f"\nNetwork statistics:")
    print(f"  Unique authors: {len(author_graph)}")
    print(f"  Total co-authorship edges: {sum(len(v) for v in author_graph.values()) // 2}")
    print(f"  Treatment authors: {sum(1 for g in author_groups.values() if g == 'treatment')}")
    print(f"  Control authors: {sum(1 for g in author_groups.values() if g == 'control')}")
    print(f"  Excluded authors: {sum(1 for g in author_groups.values() if g == 'excluded')}")
    
    return dict(author_graph), author_groups


def visualize_network(
    author_graph: Dict[str, Set[str]],
    author_groups: Dict[str, str],
    output_file: str,
    show_top_n: int = 0,
    show_clusters: bool = False
):
    """
    Create visualization of author collaboration network.
    
    Colors nodes by treatment/control groups and shows comprehensive statistics.
    
    Args:
        author_graph: Dict mapping author -> set of co-authors
        author_groups: Dict mapping author -> group assignment
        output_file: Path to save the visualization
        show_top_n: Only show top N most connected authors (0 = show all)
        show_clusters: Draw shaded boundaries around clusters
    """
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
        from matplotlib import patches as mpatches
        import numpy as np
    except ImportError:
        print("Error: networkx and matplotlib required for visualization")
        print("Install with: pip install networkx matplotlib")
        return
    
    print("\nBuilding NetworkX graph...")
    
    # Create NetworkX graph
    G = nx.Graph()
    for author, collaborators in author_graph.items():
        for collaborator in collaborators:
            G.add_edge(author, collaborator)
    
    print(f"Graph created:")
    print(f"  Nodes: {G.number_of_nodes()}")
    print(f"  Edges: {G.number_of_edges()}")
    
    # Filter to top N most connected authors if requested
    if show_top_n > 0 and G.number_of_nodes() > show_top_n:
        print(f"\nFiltering to top {show_top_n} most connected authors...")
        degrees = dict(G.degree())
        top_authors = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:show_top_n]
        top_author_names = [author for author, _ in top_authors]
        G = G.subgraph(top_author_names).copy()
        print(f"Filtered graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    # Find connected components
    components = list(nx.connected_components(G))
    print(f"\nConnected components: {len(components)}")
    if components:
        print(f"  Largest: {len(max(components, key=len))} authors")
        print(f"  Smallest: {len(min(components, key=len))} authors")
    
    # Build component map
    component_map = {}
    for i, comp in enumerate(components):
        for node in comp:
            component_map[node] = i
    
    # Calculate stats per group
    treatment_count = sum(1 for node in G.nodes() if author_groups.get(node) == 'treatment')
    control_count = sum(1 for node in G.nodes() if author_groups.get(node) == 'control')
    excluded_count = sum(1 for node in G.nodes() if author_groups.get(node) == 'excluded')
    
    # Count components by group
    treatment_components = set()
    control_components = set()
    mixed_components = set()
    
    for comp_idx, comp in enumerate(components):
        has_treatment = any(author_groups.get(node) == 'treatment' for node in comp)
        has_control = any(author_groups.get(node) == 'control' for node in comp)
        
        if has_treatment and has_control:
            mixed_components.add(comp_idx)
        elif has_treatment:
            treatment_components.add(comp_idx)
        elif has_control:
            control_components.add(comp_idx)
    
    print(f"\nCluster breakdown:")
    print(f"  Treatment-only clusters: {len(treatment_components)}")
    print(f"  Control-only clusters: {len(control_components)}")
    print(f"  Mixed clusters: {len(mixed_components)}")
    
    # Create visualization
    print("\nCreating visualization...")
    fig, ax = plt.subplots(figsize=(24, 20))
    
    # Compute layout using force-directed algorithm
    print("Computing layout (this may take a moment)...")
    # Result: Aesthetically pleasing layout showing cluster structure
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Draw cluster boundaries if requested
    if show_clusters and len(components) > 1:
        from matplotlib.patches import Circle
        from scipy.spatial import ConvexHull
        print("Drawing cluster boundaries...")
        # Draw boundary for each large component
        for comp_idx, comp in enumerate(components):
            if len(comp) < 3:  # Need at least 3 points for convex hull
                continue
            
            # Get positions of nodes in this component
            comp_pos = np.array([pos[node] for node in comp])
            
            # Only draw if component is large enough
            if len(comp) >= 3:
                try:
                    hull = ConvexHull(comp_pos)
                    # Expand hull slightly for visual clarity
                    centroid = comp_pos.mean(axis=0)
                    hull_points = comp_pos[hull.vertices]
                    expanded_hull = centroid + 1.2 * (hull_points - centroid)
                    
                    # Determine cluster type for color
                    if comp_idx in mixed_components:
                        edge_color = '#888888'
                        alpha = 0.1
                    elif comp_idx in treatment_components:
                        edge_color = '#2E86AB'
                        alpha = 0.05
                    else:  # control
                        edge_color = '#A23B72'
                        alpha = 0.05
                    
                    polygon = plt.Polygon(expanded_hull, fill=True, 
                                        facecolor=edge_color, edgecolor=edge_color,
                                        alpha=alpha, linewidth=2, linestyle='--')
                    ax.add_patch(polygon)
                except:
                    pass  # Skip if hull computation fails
    
    # Determine node colors - ALWAYS color by groups
    group_colors = {
        'treatment': '#2E86AB',  # Blue
        'control': '#A23B72',    # Purple
        'excluded': '#F18F01'    # Orange
    }
    
    node_colors = []
    node_sizes = []
    for node in G.nodes():
        group = author_groups.get(node, 'excluded')
        node_colors.append(group_colors.get(group, '#CCCCCC'))
        # Size based on degree
        degree = G.degree(node)
        node_sizes.append(50 + degree * 10)
    
    # Draw nodes
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes, 
                          alpha=0.8, ax=ax, edgecolors='white', linewidths=1.5)
    
    # Create comprehensive legend with stats
    legend_elements = [
        mpatches.Patch(color='none', label='═══ GROUPS ═══'),
        mpatches.Patch(color=group_colors['treatment'], 
                      label=f'Treatment: {treatment_count} authors'),
        mpatches.Patch(color=group_colors['control'], 
                      label=f'Control: {control_count} authors'),
    ]
    
    if excluded_count > 0:
        legend_elements.append(
            mpatches.Patch(color=group_colors['excluded'], 
                          label=f'Excluded: {excluded_count} authors')
        )
    
    legend_elements.extend([
        mpatches.Patch(color='none', label=''),
        mpatches.Patch(color='none', label='═══ CLUSTERS ═══'),
        mpatches.Patch(color='none', label=f'Total: {len(components)} clusters'),
        mpatches.Patch(color='none', label=f'Treatment-only: {len(treatment_components)}'),
        mpatches.Patch(color='none', label=f'Control-only: {len(control_components)}'),
        mpatches.Patch(color='none', label=f'Mixed: {len(mixed_components)}'),
        mpatches.Patch(color='none', label=''),
        mpatches.Patch(color='none', label='═══ NETWORK ═══'),
        mpatches.Patch(color='none', label=f'Authors: {G.number_of_nodes()}'),
        mpatches.Patch(color='none', label=f'Collaborations: {G.number_of_edges()}'),
    ])
    
    if components:
        legend_elements.append(
            mpatches.Patch(color='none', 
                          label=f'Largest cluster: {len(max(components, key=len))} authors')
        )
    
    ax.legend(handles=legend_elements, loc='upper left', fontsize=14, 
             framealpha=0.95, edgecolor='black', fancybox=True)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, alpha=0.2, ax=ax)
    
    # Draw labels for highly connected authors
    degree_dict = dict(G.degree())
    if degree_dict:
        high_degree_threshold = sorted(degree_dict.values(), reverse=True)[min(20, len(degree_dict)-1)]
        labels = {node: node for node, degree in degree_dict.items() if degree >= high_degree_threshold}
        nx.draw_networkx_labels(G, pos, labels, font_size=8, ax=ax)
    
    # Title
    title = f'Author Collaboration Network\n'
    title += f'Treatment vs Control Groups | {len(components)} Clusters\n'
    title += f'{G.number_of_nodes()} Authors | {G.number_of_edges()} Collaborations'
    ax.set_title(title, fontsize=18, pad=20, fontweight='bold')
    ax.axis('off')
    
    # Add subtitle
    subtitle = f'Node size = collaboration count | '
    if show_clusters:
        subtitle += f'Shaded regions = cluster boundaries'
    else:
        subtitle += f'Use --show-clusters to highlight cluster boundaries'
    ax.text(0.5, 0.02, subtitle, transform=fig.transFigure, 
           ha='center', fontsize=12, style='italic', color='gray')
    
    # Save
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\n✓ Visualization saved to {output_file}")
    
    # Print statistics
    print("\nTop 10 most collaborative authors:")
    if degree_dict:
        top_collaborative = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (author, degree) in enumerate(top_collaborative, 1):
            group = author_groups.get(author, 'unknown')
            print(f"  {i:2d}. {author[:40]:40s} - {degree:3d} collaborations ({group})")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize author collaboration network with Treatment/Control groups and cluster stats"
    )
    parser.add_argument(
        '--input',
        default='data/author_networks_groups.csv',
        help="Input CSV file from assign_author_groups.py (with columns: doi, author_name, author_group, doi_group)"
    )
    parser.add_argument(
        '--output',
        default='author_network_groups_clusters.png',
        help="Output PNG file for the visualization"
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=0,
        help="Only show top N most connected authors (0 = show all)"
    )
    parser.add_argument(
        '--show-clusters',
        action='store_true',
        help="Draw shaded boundaries around clusters"
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("Author Collaboration Network - Treatment/Control Groups & Clusters")
    print("="*80)
    print(f"Show cluster boundaries: {'Yes' if args.show_clusters else 'No'}")
    if args.top_n > 0:
        print(f"Filtering to top {args.top_n} most connected authors")
    print("="*80)
    
    # Load network from CSV
    author_graph, author_groups = load_author_network_from_csv(args.input)
    
    # Create visualization
    visualize_network(
        author_graph,
        author_groups,
        output_file=args.output,
        show_top_n=args.top_n,
        show_clusters=args.show_clusters
    )
    
    print("="*80)
    print("Visualization complete!")
    print("="*80)


if __name__ == "__main__":
    main()
