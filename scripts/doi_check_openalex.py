import copy
import json
import requests
from thefuzz import fuzz #TODO Add to requirements.txt
import time

# TODO - Multiple steps
# 1. Get list of papers with the same title - with year, journal name, author name fields also selected
#  1.1. (Done) Case agnostic - already by default on openalex
#  1.2. TODO Some strange errors, "Invalid" reference, looks like it's splitting on punctuation?
# 2. TODO Filter by publication year (need better querying of API)
# 3. (Done) Fuzzy search by journal name (for abbreviations)
# 4. TODO Fuzzy search on authors (for initials and name order) - Implement yourself, as Levenshtein alone won't work

#TODO Test OpenAlex accuracy by querying on objects that have a DOI in input - after fixing querying problems

# TODO Set a threshold via experimentation
FUZZY_MATCH_THRESHOLD = 75

test_mode = False

# Populate a dictionary with values from the original e.g., if no match was found on OpenAlex
def default_copy(orig, klist):
    obj_dict = dict()
    for key in klist:
        if key == 'authors': # Need to do a deep copy for lists inside dictionaries
            obj_dict[key] = copy.deepcopy(orig[key])
        else:
            obj_dict[key] = orig.get(key)
    return obj_dict

# Process the OpenAlex output into the format we want
def parse_query_results(query_output):
    #print(query_output)
    try:
        info = query_output['results']
        parsed_list = []
        if len(info) == 0:
            #print('No results found on OpenAlex...')
            return []
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
        #print('ERROR: Could not retrieve results from OpenAlex!')
        #print(etyp, emsg)
        return []

def match_title_and_year(title,year):
    url = 'https://api.openalex.org/works?filter=title.search:"{title}"&mailto=nid@dmi.dk'.format(title=title, year=year) #TODO Generic email
    #print(url)
    #TODO Paging (returns max 25 results otherwise)
    response = requests.get(url)
    try:
        res = response.json()
        #print(response.elapsed.total_seconds())
        #print(res)
        title_matches = parse_query_results(res)
        return title_matches
    except json.decoder.JSONDecodeError:
        print(response)
        return []

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

with open(fname, 'r') as f:
    data = json.load(f)

parsed_data = []

ctr = 0

for doc in data:
    print(doc['preprint']['title'])
    reflist = doc['references']
    preprint_data = copy.deepcopy(doc['preprint'])
    preprint_data['file_path'] = doc['file_path']
    
    final_data = dict()

    preprint_key_list = ['doi', 'title', 'authors', 'journal', 'published_date']
    references_key_list = ['ref_id', 'doi', 'title', 'authors', 'journal', 'year']

    if (preprint_data['has_doi'] == False) or (preprint_data['doi'] == 'null'):
        preprint_match = match_title_and_year(preprint_data['title'], preprint_data['published_date'])
        if len(preprint_match) == 0:
            final_data = default_copy(preprint_data, preprint_key_list)
        else:
            final_data = copy.deepcopy(preprint_match[0]) # TODO Multiple results from OpenAlex?
    else:
        if not test_mode: # If we are not testing, then do not waste time querying OpenAlex
            final_data = default_copy(preprint_data, preprint_key_list)

    ref_doi = []
    for ref in reflist:
        tm = dict()
        tm['ref_id'] = ref['ref_id']
        if ref['has_doi']:
            if not test_mode:
                tm = default_copy(ref, references_key_list)
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
                tm = default_copy(ref, references_key_list)
        ref_doi.append(tm)

    #print(type(final_data))
    final_data['references'] = ref_doi
    parsed_data.append(final_data)
    ctr = ctr + 1

with open(fout, 'w') as f:
    json.dump(parsed_data, f)


#title_matches = match_title_and_year("cancer", 2021) # TODO read from json
#year = 2021
#print(title_matches, len(title_matches))

#if len(title_matches) > 1:
#    year_matches = match_year(title_matches, year)
#    print(year_matches)
