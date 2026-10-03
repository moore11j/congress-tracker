"""Short, versioned product lessons. No generated financial claims."""
import copy

TUTORIALS = ('research', 'ownership', 'financials', 'congress', 'insiders', 'analysts')

# Product lessons describe a workflow, never a current-stock recommendation.
# Keep existing research/ownership scripts unchanged: approved jobs hash them.
FEATURE_LESSONS = {
    'financials': {
        'title': 'A stock is more than its price chart.',
        'hook': 'A price chart cannot tell you how the business is doing. Search for a company in Walnut and open its ticker.',
        'instruction': 'Click Financials. Compare the Revenue Trend and Earnings Trend, then check the reporting periods. This gives you a starting point for understanding the business behind the stock.',
        'headline': 'Start with the business.',
        'path': 'Financials → Revenue and earnings',
        'close': 'Use the numbers to form your next research question. Explore Walnut Markets. Research only, not investment advice.',
        'caption': 'Start stock analysis with the business: search {ticker} → Financials → compare revenue and earnings across the displayed reporting periods. A product walkthrough, not a stock recommendation. Some features require a paid plan. #StockAnalysis #InvestingTools #WalnutMarkets',
    },
    'congress': {
        'title': 'A Congress disclosure is not a live trade.',
        'hook': 'Saw a headline about a Congress trade? Search for the company in Walnut and open its ticker to check the record.',
        'instruction': 'Click Congress in Activity View. Check the trader, trade date, disclosure date, and purchase or sale. The reported amount is a range, and disclosure can come after the trade.',
        'headline': 'Check the dates and direction.',
        'path': 'Activity View → Congress',
        'close': 'Use the filing context before drawing a conclusion. Explore Walnut Markets. Research only, not investment advice.',
        'caption': 'Check the record behind a Congress-trading headline: search {ticker} → Activity View → Congress. Compare trade and disclosure dates, direction, and reported value ranges. Disclosed activity is not live trading. Some features require a paid plan. #CongressTrades #StockResearch #WalnutMarkets',
    },
    'insiders': {
        'title': 'Who traded: a CEO, director, or someone else?',
        'hook': 'An insider-trading headline leaves out context. Search for the company in Walnut and open its ticker to investigate.',
        'instruction': 'Click Insiders in Activity View. Check the person, their role, the trade date, filing date, and transaction type. Read the record before treating the activity as a signal.',
        'headline': 'The person and dates matter.',
        'path': 'Activity View → Insiders',
        'close': 'A reported trade is one input, not a reason to copy it. Explore Walnut Markets. Research only, not investment advice.',
        'caption': 'Put insider activity in context: search {ticker} → Activity View → Insiders. Check the person, role, dates, and transaction type. One reported trade does not establish intent or predict returns. Some features require a paid plan. #InsiderTrading #StockAnalysis #WalnutMarkets',
    },
    'analysts': {
        'title': 'One price target is not the whole story.',
        'hook': 'One analyst price target is not the whole story. Search for the company in Walnut and open its ticker.',
        'instruction': 'Click Analysts. Compare the rating distribution and Price Targets range, then check the snapshot date and coverage. This shows the spread of expectations instead of just one headline number.',
        'headline': 'See the range of expectations.',
        'path': 'Analysts → Ratings and price targets',
        'close': 'Targets are opinions, not promised returns. Explore Walnut Markets. Research only, not investment advice.',
        'caption': 'Look beyond one price target: search {ticker} → Analysts → inspect ratings, the target range, coverage, and snapshot date. Analyst expectations are not guaranteed outcomes. Some features require a paid plan. #StockAnalysis #AnalystRatings #WalnutMarkets',
    },
}


def adapt(board, tutorial, version=None):
    if tutorial not in TUTORIALS:
        raise ValueError('Unknown feature tutorial.')
    if version is None:
        version = 2 if tutorial in {'congress', 'insiders'} else 1
    if version not in {1, 2} or (version == 2 and tutorial not in {'congress', 'insiders'}):
        raise ValueError('Unknown feature tutorial version.')
    board = copy.deepcopy(board)
    ticker = board['ticker']
    ticker_url = f'https://app.walnutmarkets.com/ticker/{ticker}'
    if tutorial == 'research':
        title = 'Too many stock tabs? Start here.'
        beats = [
            ('daily_search', 'Too many stock tabs open? Search for a company in Walnut and open its ticker.', 'Too many stock tabs?', f'Start with one company · {ticker}', ticker_url),
            ('daily_research', 'Click Research to find the briefs attached to this company, instead of starting another search.', 'One company. Its research.', f'{ticker} → Research', ticker_url),
            ('daily_brief', 'Open the brief. Read its dated evidence and risks before deciding what deserves a closer look.', 'Get context before a call.', 'Open the dated analysis', board['target_url']),
            ('daily_cta', 'Give your next stock question a starting point. Explore Walnut Markets.', 'Start with context.', 'walnutmarkets.com', None),
        ]
        caption = f'Too many stock tabs? Try this three-step Walnut workflow: search {ticker}, open Research, then read the brief and its risks. One company is the starting point—not a recommendation. Research only; some features require a paid plan. #StockResearch #InvestingTools #WalnutMarkets'
    elif tutorial == 'ownership':
        title = 'Who holds a stock—and when?'
        beats = [
            ('daily_search', 'Who actually holds a stock? Search for a company in Walnut and open its ticker.', 'Who holds this stock?', f'A quick ownership check · {ticker}', ticker_url),
            ('tutorial_ownership', 'Click Ownership to inspect the reported holders and reporting period. A filing describes past holdings, not purchases happening right now.', 'Holdings need a date.', f'{ticker} → Ownership', ticker_url),
            ('daily_cta', 'Use the ownership view to add context to your research, not as a buy signal. Explore Walnut Markets.', 'Context before conviction.', 'walnutmarkets.com', None),
        ]
        caption = f'Before interpreting institutional activity, check who reported the holding and which period it covers. Search {ticker} → Ownership in Walnut. Reported holdings are historical snapshots, not live trades or a buy signal. Product tutorial; some features require a paid plan. #StockResearch #InstitutionalOwnership #WalnutMarkets'
    else:
        lesson = dict(FEATURE_LESSONS[tutorial])
        if version == 2 and tutorial == 'congress':
            lesson['instruction'] = 'Click Congress in Activity View. Check the trader, displayed date, and reported value range. Click Buys, then Sells to separate direction. A disclosure can arrive after the actual trade.'
            lesson['caption'] = 'Check the record behind a Congress-trading headline: search {ticker} → Activity View → Congress. Inspect the displayed date and value range; use Buys and Sells to separate direction. Disclosures can lag trades. Some features require a paid plan. #CongressTrades #StockResearch #WalnutMarkets'
        elif version == 2 and tutorial == 'insiders':
            lesson['instruction'] = 'Click Insiders in Activity View. Check the person, role, and filed date. Click Buys, then Sells to separate direction. A filing date is not necessarily the date of the trade.'
            lesson['caption'] = 'Put insider activity in context: search {ticker} → Activity View → Insiders. Check the person, role, and filed date; use Buys and Sells to separate direction. Filing dates can differ from trade dates. Some features require a paid plan. #InsiderTrading #StockAnalysis #WalnutMarkets'
        title = lesson['title']
        beats = [
            ('daily_search', lesson['hook'], title, f'Start with one company · {ticker}', ticker_url),
            ('tutorial_' + tutorial, lesson['instruction'], lesson['headline'], lesson['path'], ticker_url),
            ('daily_cta', lesson['close'], 'Try one research task.', 'walnutmarkets.com', None),
        ]
        caption = lesson['caption'].format(ticker=ticker)
    scenes = []
    for i, (shot, voice, headline, subhead, url) in enumerate(beats, 1):
        scenes.append(dict(sequence=i, shot=shot, narration=voice, on_screen_text=headline,
            subhead=subhead, duration_seconds=max(3,round(len(voice.split())/2.5)), walnut_url=url,
            capture_target=shot,capture_action='record' if url else 'none',
            visual_type='walnut_recording' if url else 'brand_card',transition='cut',
            statement_ids=[shot],evidence_ids=['product_tutorial:'+tutorial]))
    board.update(content_kind='feature_tutorial',tutorial_id=tutorial,tutorial_version=version,
        tutorial_title=title,format='product_investigation',creative_angle='feature_tutorial',
        hook=beats[0][1],hook_id='tutorial_'+tutorial,storyboard=scenes,scenes=scenes,
        narration=' '.join(s['narration'] for s in scenes),
        target_duration_seconds=sum(s['duration_seconds'] for s in scenes),
        caption=caption,first_comment='',cta='Explore Walnut Markets.',
        posting_notes='Feature tutorial in the 3 research / 1 tutorial mix. Review the actual demo before scheduling to Instagram and TikTok.')
    return board
