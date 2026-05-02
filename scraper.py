import re
import shelve
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict
import warnings
from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

unique_pages = set()
longest_page = {"url": "", "word_count": 0}
word_counts = defaultdict(int)
subdomain_pages = defaultdict(set)

STOPWORDS = frozenset({
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "arent", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "cant",
    "cannot", "could", "couldnt", "did", "didnt", "do", "does", "doesnt",
    "doing", "dont", "down", "during", "each", "few", "for", "from",
    "further", "had", "hadnt", "has", "hasnt", "have", "havent", "having",
    "he", "hed", "hell", "hes", "her", "here", "heres", "hers", "herself",
    "him", "himself", "his", "how", "hows", "i", "id", "ill", "im", "ive",
    "if", "in", "into", "is", "isnt", "it", "its", "itself", "lets",
    "me", "more", "most", "mustnt", "my", "myself", "no", "nor", "not", "of",
    "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shant", "she", "shed",
    "shell", "shes", "should", "shouldnt", "so", "some", "such", "than",
    "that", "thats", "the", "their", "theirs", "them", "themselves", "then",
    "there", "theres", "these", "they", "theyd", "theyll", "theyre",
    "theyve", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasnt", "we", "wed", "well", "were", "weve",
    "werent", "what", "whats", "when", "whens", "where", "wheres", "which",
    "while", "who", "whos", "whom", "why", "whys", "with", "wont", "would",
    "wouldnt", "you", "youd", "youll", "youre", "youve", "your", "yours",
    "yourself", "yourselves",
})

MIN_PAGE_TOKENS = 50

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200:
        return list()
    if resp.raw_response is None:
        return list()
    if resp.raw_response.content is None:
        return list()
    
    soup = BeautifulSoup(resp.raw_response.content, 'lxml')
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    text = soup.get_text(separator=' ', strip=True)
    tokens = tokenize_text(text)
    
    parsed_url = urlparse(resp.raw_response.url)
    defrag_url = urldefrag(resp.raw_response.url)[0]

    host = parsed_url.hostname or ""
    allowed_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
    if not any(host == domain or host.endswith("." + domain) for domain in allowed_domains):
        return list()
    
    unique_pages.add(defrag_url)
    if parsed_url.hostname:
        subdomain_pages[parsed_url.hostname].add(defrag_url)
    if len(tokens) < MIN_PAGE_TOKENS:
        return list()

    if len(tokens) > longest_page["word_count"]:
        longest_page["url"] = defrag_url
        longest_page["word_count"] = len(tokens)

    for token in tokens:
        if len(token) > 1 and token not in STOPWORDS:
            word_counts[token] += 1

    
    if len(unique_pages) % 50 == 0:
        save_analytics()
    
    extracted_links = list()
    for link in soup.find_all('a', href=True):
        href = link['href'].strip()
        if not href or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("#"):
            continue
        absolute_url = urljoin(resp.raw_response.url, href)
        link_url = urldefrag(absolute_url)[0]
        extracted_links.append(link_url)
    
    return extracted_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        host = parsed.hostname
        if host is None:
            return False
        allowed_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        if not any(host == domain or host.endswith("." + domain) for domain in allowed_domains):
            return False

        # URL too long
        if len(url) > 200:
            return False
        
        segments = [segment for segment in parsed.path.split('/') if segment]
        # too many segments
        if len(segments) > 8:
            return False
        # repeated path segment
        for segment in segments:
            if segments.count(segment) > 2:
                return False

        # too many query parameters
        if len(parsed.query.split('&')) > 5:
            return False
        
        # Calender/event pages
        if re.search(r"/\d{4}[-/]\d{2}([-/]\d{2})?/", parsed.path):
            return False
        if re.search(r"(calendar|events?)", parsed.path.lower()) and re.search(r"\d{4}", parsed.path):
            return False
        if re.search(r"/events?/(list|week|month|today|day|year)/", parsed.path.lower()):
            return False
        
        # specific known traps
        # UCI ML dataset archive
        blocked_subdomains = ("archive.ics.uci.edu", "archive-beta.ics.uci.edu", "wiki.ics.uci.edu", "swiki.ics.uci.edu", "grape.ics.uci.edu")
        if host in blocked_subdomains:
            return False
        # Paginated archive listings
        if re.search(r"/page/\d+/?$", parsed.path):
            return False
        # DokuWiki action/index
        if "doku.php" in parsed.path.lower():
            if "do=" in parsed.query.lower() or "idx=" in parsed.query.lower():
                return False
        # iCal export of events
        if "ical=1" in parsed.query.lower():
            return False
        # Elementor draft pages
        if re.search(r"/elementor-\d+", parsed.path.lower()):
            return False
        if re.search(r"C=[NSDM][;&]O=[AD]", parsed.query):
            return False
        if "/~dhirschb/genealogy/" in parsed.path.lower():
            return False
    
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|pps|ppsx"
            + r"|odp|odt|ods|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz"
            + r"|c|cc|cpp|h|hpp|java|py|m"
            + r"|txt|lif|scm|lsp|hs|rkt|ss|sh|ff"
            + r"|dsw|dsp|mht|mhcid|ipynb|json"
            + r"|defs|dirs|path|inc|als|cls|nb|ma|pov"
            + r")$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def tokenize_text(text):
    tokens = []
    current_token = ""
    for char in text:
        if char.isalnum() and char.isascii():
            current_token += char.lower()
        else:
            if current_token:
                tokens.append(current_token)
                current_token = ""
    
    if current_token:
        tokens.append(current_token)
    return tokens

def save_analytics():
    with shelve.open('crawler_data') as db:
        db['unique_pages'] = unique_pages
        db['longest_page'] = longest_page
        db['word_counts'] = dict(word_counts)
        db['subdomain_pages'] = dict(subdomain_pages)

def load_analytics():
    global unique_pages, longest_page, word_counts, subdomain_pages
    try:
        with shelve.open('crawler_data') as db:
            unique_pages = db.get('unique_pages', set())
            longest_page = db.get('longest_page', {"url": "", "word_count": 0})
            word_counts = defaultdict(int, db.get('word_counts', dict()))
            subdomain_pages = defaultdict(set, db.get('subdomain_pages', dict()))
    except Exception as e:
        import sys
        print(f"[load_analytics] could not load saved state: {e}", file=sys.stderr)

load_analytics()