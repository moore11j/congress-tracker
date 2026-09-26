"""Short, versioned product lessons. No generated financial claims."""
import copy

TUTORIALS = ('research', 'ownership')


def adapt(board, tutorial):
    if tutorial not in TUTORIALS:
        raise ValueError('Unknown feature tutorial.')
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
    else:
        title = 'Who holds a stock—and when?'
        beats = [
            ('daily_search', 'Who actually holds a stock? Search for a company in Walnut and open its ticker.', 'Who holds this stock?', f'A quick ownership check · {ticker}', ticker_url),
            ('tutorial_ownership', 'Click Ownership to inspect the reported holders and reporting period. A filing describes past holdings, not purchases happening right now.', 'Holdings need a date.', f'{ticker} → Ownership', ticker_url),
            ('daily_cta', 'Use the ownership view to add context to your research, not as a buy signal. Explore Walnut Markets.', 'Context before conviction.', 'walnutmarkets.com', None),
        ]
        caption = f'Before interpreting institutional activity, check who reported the holding and which period it covers. Search {ticker} → Ownership in Walnut. Reported holdings are historical snapshots, not live trades or a buy signal. Product tutorial; some features require a paid plan. #StockResearch #InstitutionalOwnership #WalnutMarkets'
    scenes = []
    for i, (shot, voice, headline, subhead, url) in enumerate(beats, 1):
        scenes.append(dict(sequence=i, shot=shot, narration=voice, on_screen_text=headline,
            subhead=subhead, duration_seconds=max(3,round(len(voice.split())/2.5)), walnut_url=url,
            capture_target=shot,capture_action='record' if url else 'none',
            visual_type='walnut_recording' if url else 'brand_card',transition='cut',
            statement_ids=[shot],evidence_ids=['product_tutorial:'+tutorial]))
    board.update(content_kind='feature_tutorial',tutorial_id=tutorial,tutorial_version=1,
        tutorial_title=title,format='product_investigation',creative_angle='feature_tutorial',
        hook=beats[0][1],hook_id='tutorial_'+tutorial,storyboard=scenes,scenes=scenes,
        narration=' '.join(s['narration'] for s in scenes),
        target_duration_seconds=sum(s['duration_seconds'] for s in scenes),
        caption=caption,first_comment='',cta='Explore Walnut Markets.',
        posting_notes='Feature tutorial in the 3 research / 1 tutorial mix. Review the actual demo before scheduling to Instagram and TikTok.')
    return board
