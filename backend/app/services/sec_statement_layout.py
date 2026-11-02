"""Explicit adjacent layout-table captions; never ownership from distant prose."""
import re

POLICY='adjacent_statement_layout_v1'
RETRY_REASONS={'financial_hint_not_qualified','table_accounting_basis_unproved'}


def period_text(text):
    text=re.sub(r'\b(three|six|nine|twelve)- and (three|six|nine|twelve)-month periods? ended\b',
                r'\1 and \2 months ended',text,flags=re.I)
    return re.sub(r'\b(three|six|nine|twelve)-month periods? ended\b',r'\1 months ended',text,flags=re.I)


def context(table,issuer):
    from app.services import sec_tabular_financials as tab
    # Stay inside the same DOM parent and cross only empty layout paragraphs.
    for index,node in enumerate(table.itersiblings(preceding=True)):
        if index>=4:return None
        text=tab.visible_text(node)
        if not text and node.tag in {'p','div'} and not node.xpath('.//table'):continue
        if node.tag!='table' or node.xpath('.//table') or node.xpath('ancestor::blockquote'):return None
        rows=tab.grid(node)
        if len(rows)!=1 or sum(bool(c['text']) for c in rows[0])!=1:return None
        blocks=[tab.visible_text(n) for n in node.xpath('.//p') if tab.visible_text(n)]
        if len(blocks) not in {3,4} or len(text)>700:return None
        if tab.table_kind(blocks[1]) is None:return None
        from app.services.sec_statement_declarations import unnamed_heading
        if (not unnamed_heading(blocks[1]) or re.search(r'non.?ifrs|non.?gaap|adjusted|pro.?forma|notes to',text,re.I)
                or tab.company_key(blocks[0])!=tab.company_key(issuer)):
            raise tab.TabularFinancialError('layout_statement_owner_unproved')
        # Dates are still independently validated against every printed column.
        if len(blocks)==4 and not re.fullmatch(
                r'For the (?:three|six|nine|twelve)(?:- and (?:three|six|nine|twelve))?-month periods? ended '
                r'[A-Za-z]+ \d{1,2}, 20\d{2}(?: and 20\d{2})?',blocks[2],re.I):
            raise tab.TabularFinancialError('layout_period_caption_unproved')
        if not re.fullmatch(r'\(All amounts in (?:thousands|millions) of reais(?: unless otherwise stated)?\)',blocks[-1],re.I):
            raise tab.TabularFinancialError('layout_currency_caption_unproved')
        return blocks
    return None


def has_caption(tree,issuer):
    from app.services.sec_tabular_financials import TabularFinancialError
    for table in tree.xpath('//table[not(ancestor::blockquote)]'):
        try:
            if context(table,issuer):return True
        except TabularFinancialError:
            return True  # The full extraction must retain, not evade, this hold.
    return False


def currency_scope(tree,issuer):
    from app.services import sec_tabular_financials as tab
    # Native currency must be explicitly spelled out in the same document.
    # A bare dollar/reais marker or the issuer's country cannot select it.
    nodes=tree.xpath('//*[self::p or self::div][not(ancestor::blockquote)]'
        '[not(.//table)][not(.//p)][not(.//div)]')
    from app.services.sec_statement_currency import ALIASES
    found=[]
    for node in nodes:
        text=tab.visible_text(node)
        for code,alias in ALIASES.items():
            pattern=(r'\bThe consolidated financial statements are presented in '
                     r'(?P<scale>thousands|millions) of '+alias+r', unless otherwise indicated, '
                     r'which is the functional currency of')
            for match in re.finditer(pattern,text,re.I):
                prefix=text[max(0,match.start()-70):match.start()]
                if re.search(r'\b(?:not|never|unlike|hypothetical|if)\s+(?:\w+\s+){0,6}$',prefix,re.I):continue
                found.append(dict(currency=code,scale=1000 if match['scale'].lower()=='thousands' else 1_000_000,
                    declaration=match.group(0)))
    if not found or {(f['currency'],f['scale']) for f in found}!={('BRL',found[0]['scale'])}:return None
    return dict(issuer=issuer,currency='BRL',scale=found[0]['scale'],basis='explicit_statement_currency',
        declarations=[dict(t) for t in dict.fromkeys(tuple(f.items()) for f in found)])
