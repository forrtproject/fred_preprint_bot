import copy
import json
import requests
from thefuzz import fuzz #TODO Add to requirements.txt

# TODO - Multiple steps
# 1. (Done) Get list of papers with the same title - with year, journal name, author name fields also selected
#  1.1. (Done) Case agnostic - already by default on openalex
# 2. (Done) Filter by publication year (no querying the API again)
# 3. (Done) Fuzzy search by journal name (for abbreviations)
# 4. Fuzzy search on authors (for initials and name order)

# TODO Set a threshold via experimentation
FUZZY_MATCH_THRESHOLD = 75

def read_from_json(fname):
    with open(fname, 'r') as f:
        data = json.load(f)
    reflist = data[0]['references']
    preprint_data = copy.deepcopy(data[0]['preprint'])
    preprint_data['file_path'] = data[0]['file_path']
    return preprint_data, reflist

def parse_query_results(query_output):
    #print(query_output)
    try:
        info = query_output['results']
        parsed_list = []
        if len(info) == 0:
            print('No results found on OpenAlex...')
        for result in info:
            #print(result)
            obj_dict = dict()
            obj_dict['doi'] = result['doi']
            obj_dict['openalex_id'] = result['id']
            obj_dict['title'] = result['title']
            obj_dict['year'] = result['publication_year']
            if result['primary_location']:
                if result['primary_location']['source']:
                    if result['primary_location']['source']['display_name']:
                        obj_dict['journal'] = result['primary_location']['source']['display_name']
                    else:
                        obj_dict['journal'] = None
                else:
                    obj_dict['journal'] = None
            else:
                obj_dict['journal'] = None
            
            author_list_raw = result['authorships']
            obj_dict['author'] = []
            for author in author_list_raw:
                obj_dict['author'].append(author['author']['display_name'])
            
            parsed_list.append(obj_dict)
        return parsed_list
    except KeyError:
        etyp = query_output['error']
        emsg = query_output['message']
        print('ERROR: Could not retrieve results from Open Alex!')
        print(etyp, emsg)
        return []

def match_title_and_year(title,year):
    url = 'https://api.openalex.org/works?filter=title.search:"{title}"&mailto=nid@dmi.dk'.format(title=title, year=year) #TODO Generic email
    #print(url)
    #TODO Paging (returns max 25 results otherwise)
    res = requests.get(url).json()
    #print(res)
    title_matches = parse_query_results(res)
    return title_matches

def match_year(title_matches, year):
    year_matches = []
    for result in title_matches:
        res_year = result['year']
        if res_year == year:
            year_matches.append(result)
    return year_matches

def fuzzy_match_journal(year_matches, journal):
    max_similarity = 0 # TODO Select the highest or all above threshold? Or both?
    journal_matches = []
    for result in year_matches:
        obj_dict = copy.deepcopy(result) # Deep copy to avoid modifying the input
        res_journal = result['journal']
        similarity = fuzz.ratio(res_journal, journal)
        obj_dict['journal'] = similarity
        journal_matches.append(obj_dict)
        assert 'match' not in result # Sanity check to make sure we created a new object
    return journal_matches

def fuzzy_match_authors(journal_matches, author_list):
    # TODO Multiple types of fuzzy matching (order and abbreviations)
    # TODO Save the match scores for each comparison to find
    # TODO Author list converted to dictionary with different match scores for each author
    max_similarity = 0 # TODO Select the highest or all above threshold? Or both?
    author_matches = []
    for result in journal_matches:
        obj_dict = copy.deepcopy(result)
        obj_dict['author_list'] = dict()
        assert isinstance(result['author_list'], list) # Sanity check to make sure we created a new object
        res_author_list = result['author_list']
        if len(author_list) == len(res_author_list):
            matched_author_list = '' # To make sure that one author name is not matched to multiple
            for a1 in author_list:
                max_basic = 0
                max_order = 0
                matched_author = ''
                for a2 in res_author_list:
                    similarity_basic = fuzz.ratio(a1, a2)
                    similarity_order = fuzz.token_sort_ratio(a1, a2)
                    if similarity_basic > max_basic or similarity_order > max_order: # If it is the same order then either reordering or initialization should give higher scores
                        max_basic = similarity_basic
                        max_order = similarity_order
                        matched_author = 'a2'
                matched_author_list.append(matched_author)
                obj_dict['author_list'][a1] = (matched_author, max_basic, max_order)
            if len(set(matched_author_list)) == len(author_list): # To make sure one author is not falsely matched to multiple
                author_matches.append(obj_dict)
    return author_matches

fname = '../data/preprints_with_references.json' #TODO better folder traversal
fout = '../data/openalex_doi_matched_preprints_with_references.json'

dat, reflist = read_from_json(fname)

final_data = dict()

if (dat['has_doi'] == False) or (dat['doi'] == 'null'):
    preprint_match = match_title_and_year(dat['title'], dat['published_date'])
    if len(preprint_match) == 0:
        final_data['title'] = dat['title']
        final_data['author'] = dat['authors']
        final_data['journal'] = None
        final_data['published_date'] = dat['published_date']
        final_data['doi'] = None
    else:
        final_data = copy.deepcopy(preprint_match[0]) #TODO Same as below, check which result has a DOI
else:
    final_data['title'] = dat['title']
    final_data['author'] = dat['authors']
    final_data['journal'] = None
    final_data['published_date'] = dat['published_date']
    final_data['doi'] = dat['doi']

ref_doi = []
for ref in reflist:
    tm = dict()
    tm['ref_id'] = ref['ref_id']
    if ref['has_doi']:
        tm['title'] = ref['title']
        tm['author'] = copy.deepcopy(ref['authors'])
        tm['year'] = ref['year']
        tm['journal'] = ref['journal']
        tm['doi'] = ref['doi']
    else:
        title_matches = match_title_and_year(ref['title'], ref['year'])
        #print(ref['title'], len(title_matches))
        if len(title_matches) > 0:
            tm0 = title_matches[0]
            if tm0['doi'] is None:
                for pm in title_matches[1:]:
                    if pm['doi'] is not None:
                        tm0 = pm
            for key in tm0:
                tm[key] = tm0[key]
        else:
            tm['title'] = ref['title']
            tm['author'] = copy.deepcopy(ref['authors'])
            tm['year'] = ref['year']
            tm['journal'] = ref['journal']
            tm['doi'] = ref['doi']
    ref_doi.append(tm)

final_data['references'] = ref_doi

with open(fout, 'w') as f:
    json.dump(final_data, f)


#title_matches = match_title_and_year("cancer", 2021) # TODO read from json
#year = 2021
#print(title_matches, len(title_matches))

#if len(title_matches) > 1:
#    year_matches = match_year(title_matches, year)
#    print(year_matches)
