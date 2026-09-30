import copy
import inspect

import pytest
from PIL import Image, ImageChops

from app.services.growth_motion_layout import SAFE, PANEL, MotionStyle, decorate, wrapped
from app.services.growth_navigation_render import render_navigation_video


@pytest.mark.parametrize('closing', [False, True])
@pytest.mark.parametrize('elapsed', [0, .11, .22, 2])
def test_motion_copy_stays_in_social_safe_area(closing, elapsed):
    image = Image.new('RGB', (1080, 1920), 'black')
    baseline = image.copy()
    scene = {'sequence': 1, 'start': 0, 'end': 4,
             'on_screen_text': 'Who increased their Microsoft holdings?',
             'subhead': 'MSFT → Research · Snapshot 2026-09-25'}
    creative = {'ticker': 'MSFT'}
    before = copy.deepcopy((scene, creative))
    decorate(image, scene, [scene], creative, {'text': 'SEC filings, and what'},
             elapsed=elapsed, closing=closing, logo=Image.new('RGB', (100, 100), 'green'))
    bounds = ImageChops.difference(image, baseline).getbbox()
    assert bounds[0] >= SAFE[0] and bounds[1] >= SAFE[1]
    assert bounds[2] <= SAFE[2] and bounds[3] <= SAFE[3]
    assert (scene, creative) == before
    assert PANEL[3] < 1060


def test_motion_does_not_paint_over_source_footage_or_fade_captions():
    scene = {'sequence': 1, 'start': 0, 'end': 4, 'on_screen_text': 'Read the research.', 'subhead': 'MSFT → Research'}
    style = MotionStyle((1080, 1920))
    frames = []
    for elapsed in (0, .11, .65, 2):
        image = style.frame(elapsed, elapsed)
        pic = Image.new('RGB', (824, 490), '#1267af')
        style.panel(image, pic, PANEL[:2])
        before = image.crop(PANEL)
        decorate(image, scene, [scene], {'ticker': 'MSFT'}, {'text': 'Reported shares, not live purchases'}, elapsed=elapsed)
        assert ImageChops.difference(image.crop(PANEL), before).getbbox() is None
        frames.append(image)
    assert ImageChops.difference(frames[0].crop((48, 1050, 912, 1200)), frames[1].crop((48, 1050, 912, 1200))).getbbox() is None
    assert ImageChops.difference(frames[0], frames[-1]).getbbox() is not None
    assert style.frame(0, 0).getpixel((80, 520)) != (18, 103, 175)


def test_headings_preserve_every_word_and_reject_unbounded_copy():
    original = 'Reported institutional holdings increased during the latest filing period'
    lines, size = wrapped(original, 60, 800, 2)
    assert ' '.join(lines) == original
    assert 16 <= size <= 60
    with pytest.raises(ValueError, match='safe text area'):
        wrapped('overlong ' * 500, 60, 800, 2)


def test_production_renderer_defaults_to_approved_motion_layout():
    assert inspect.signature(render_navigation_video).parameters['presentation'].default == 'motion_v1'
