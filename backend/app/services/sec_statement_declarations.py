"""Explicit statement declarations, never inferred from country or filer alone."""
import re

POLICY = 'explicit_ifrs_statement_declarations_v1'
IFRS = r'(?:IAS\s+34(?:\s+Interim Financial Reporting)?|International Accounting Standard\s+34|IFRS(?:\s+Accounting Standards)?(?!\s*\d)|International Financial Reporting Standards)'
SUBJECT = r'(?:financial statements|financial information|financial performance|results)'
VERB = r'(?:prepared(?: and presented)?|presented) in accordance with'


def declarations(text):
    pattern = SUBJECT+r'.{0,240}?\b'+VERB+r'\s+'+IFRS+r'\b'
    found = []
    for match in re.finditer(pattern,text,re.I):
        value=match.group(0)
        prefix=text[max(0,match.start()-70):match.start()]
        if re.search(r'\b(?:not|never|unlike|hypothetical|will|would|could|should|expected|planned)\b',value,re.I):continue
        if re.search(r'\b(?:not|never|unlike|hypothetical)\s+(?:\w+\s+){0,6}$',prefix,re.I):continue
        # Do not cross an ordinary sentence boundary. Issuer abbreviations
        # such as S.A. before a lower-case "for" are part of the same sentence.
        if re.search(r'[.;]\s+[A-Z][a-z]{2,}\b',value):continue
        found.append(value)
    if found:return list(dict.fromkeys(found))
    pattern=re.compile(SUBJECT+r'.{0,240}?\b'+VERB+r'\s+(?:the\s+)?'+IFRS+r'\b',re.I)
    for subject in re.finditer(SUBJECT,text,re.I):
        match=pattern.match(text,subject.start())
        if not match:continue
        value=match.group(0);prefix=text[max(0,match.start()-70):match.start()]
        if re.search(r'\b(?:not|never|unlike|hypothetical|will|would|could|should|expected|planned)\b',value,re.I):continue
        if re.search(r'\b(?:not|never|unlike|hypothetical)\s+(?:\w+\s+){0,6}$',prefix,re.I):continue
        if re.search(r'[.;]\s+[A-Z][a-z]{2,}\b',value):continue
        found.append(value)
    return list(dict.fromkeys(found))


def named_statement_scope(text,issuer,company_key):
    # Ownership must occur in the actual preparation declaration. A filer
    # header, URL, annual-report reference or nearby company name is insufficient.
    pattern=(r'(?:The )?(?:interim )?(?:condensed )?consolidated financial statements of '
             r'(?P<issuer>.{1,140}?) for (?P<period>[^;]{1,150}?) have been '
             +VERB+r'\s+'+IFRS+r'\b')
    matches=list(re.finditer(pattern,text,re.I))
    qualified=[m for m in matches if declarations(m.group(0))
               and not re.search(r'\b(?:not|never|unlike|hypothetical)\s+(?:\w+\s+){0,6}$',
                                 text[max(0,m.start()-70):m.start()],re.I)]
    if not qualified or any(company_key(m['issuer'])!=company_key(issuer) for m in qualified):return None
    return {'issuer':issuer,'declaration':qualified[0].group(0),
            'basis':'explicit_named_statement_preparation','policy':POLICY}


def unnamed_heading(text):
    """A bare statement title cannot carry an unverified trailing entity."""
    return bool(re.fullmatch(
        r'(?:(?:unaudited|audited|interim|condensed) )*consolidated '
        r'(?:(?:interim|condensed) )*(?:balance sheets?|statements? of '
        r'(?:financial position|operations|income|profit or loss|comprehensive (?:loss|income)|cash flows?))'
        r'(?: \(unaudited\))?', text, re.I))


def neutral_context(text,table_kind,statement_prefix,layout_context):
    if layout_context(text):return True
    if re.fullmatch(r'(?:Table of contents|PART [IVX]+ - FINANCIAL INFORMATION|Item 1\. Financial Statements|\(?Unaudited\)?|-\s*\d{1,3}\s*-|F-\d{1,3})',text,re.I):return True
    if re.fullmatch(r'For the (?:three|six|nine|twelve)(?: and (?:three|six|nine|twelve))? months ended [A-Za-z]+ \d{1,2}, 20\d{2}',text,re.I):return True
    if re.fullmatch(r'The accompanying notes are an integral part of the (?:interim )?(?:condensed )?consolidated financial statements\.',text,re.I):return True
    if re.fullmatch(r'\(?(?:(?:CHF|EUR|USD) )?in (?:[€£¥] |(?:CHF|EUR|USD) )?(?:millions|thousands)(?:, except (?:share and per share|per share|share|earnings per share) (?:data|amounts))?\)?',text,re.I):return True
    return unnamed_heading(text)
