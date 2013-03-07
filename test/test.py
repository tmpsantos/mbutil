import os, shutil
from nose import with_setup
from mbutil import mbtiles_to_disk, disk_to_mbtiles, fill_mbtiles

def clear_data():
    try:
        shutil.rmtree('test/output')
    except Exception:
        pass

    os.mkdir('test/output')


@with_setup(clear_data, clear_data)
def test_mbtiles_to_disk():
    mbtiles_to_disk('test/data/one_tile.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/0/0/0.png')
    assert os.path.exists('test/output/metadata.json')


@with_setup(clear_data, clear_data)
def test_mbtiles_to_disk_with_bad_zoom():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', min_zoom=0, max_zoom=2, bbox='-180,-90,180,90')
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output', zoom=3)
    assert os.path.exists('test/output/metadata.json')
    assert not os.path.exists('test/output/tiles/0/0/0.png')
    assert not os.path.exists('test/output/tiles/3/0/0.png')


@with_setup(clear_data, clear_data)
def test_mbtiles_to_disk_with_zoom():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', min_zoom=0, max_zoom=2, bbox='-180,-90,180,90')
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output', zoom=1)
    assert os.path.exists('test/output/metadata.json')
    assert os.path.exists('test/output/tiles/1/0/0.png')
    assert os.path.exists('test/output/tiles/1/0/1.png')
    assert os.path.exists('test/output/tiles/1/1/0.png')
    assert os.path.exists('test/output/tiles/1/1/1.png')
    assert not os.path.exists('test/output/tiles/0/0/0.png')
    assert not os.path.exists('test/output/tiles/3/0/0.png')


@with_setup(clear_data, clear_data)
def test_mbtiles_to_disk_and_back():
    mbtiles_to_disk('test/data/one_tile.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/0/0/0.png')
    disk_to_mbtiles('test/output', 'test/output/one.mbtiles')
    assert os.path.exists('test/output/one.mbtiles')


@with_setup(clear_data, clear_data)
def test_mbtiles_fill():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', zoom=1, tile_bbox='0,0,1,0')
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/1/0/0.png')
    assert os.path.exists('test/output/tiles/1/1/0.png')
    assert not os.path.exists('test/output/tiles/1/0/1.png')
    assert not os.path.exists('test/output/tiles/1/1/1.png')


@with_setup(clear_data, clear_data)
def test_mbtiles_fill_min_max():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', min_zoom=1, max_zoom=2, bbox='0.1,0.1,180,90')
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/1/1/0.png')
    assert not os.path.exists('test/output/tiles/1/0/0.png')
    assert not os.path.exists('test/output/tiles/1/1/1.png')
    assert not os.path.exists('test/output/tiles/1/2/0.png')
    assert os.path.exists('test/output/tiles/2/2/0.png')
    assert os.path.exists('test/output/tiles/2/2/1.png')
    assert os.path.exists('test/output/tiles/2/3/0.png')
    assert os.path.exists('test/output/tiles/2/3/1.png')
    assert not os.path.exists('test/output/tiles/2/2/2.png')
    assert not os.path.exists('test/output/tiles/2/1/1.png')
    assert not os.path.exists('test/output/tiles/2/0/0.png')


@with_setup(clear_data, clear_data)
def test_mbtiles_fill_flipy():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', zoom=1, tile_bbox='0,0,1,0', flip_y=True)
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/1/0/1.png')
    assert os.path.exists('test/output/tiles/1/1/1.png')
    assert not os.path.exists('test/output/tiles/1/0/0.png')
    assert not os.path.exists('test/output/tiles/1/1/0.png')


@with_setup(clear_data, clear_data)
def test_mbtiles_fill_min_max_flipy():
    fill_mbtiles('test/output/fill.mbtiles', 'test/data/tile.png', min_zoom=1, max_zoom=2, bbox='0.1,0.1,180,90', flip_y=True)
    assert os.path.exists('test/output/fill.mbtiles')
    mbtiles_to_disk('test/output/fill.mbtiles', 'test/output')
    assert os.path.exists('test/output/tiles/1/1/1.png')
    assert not os.path.exists('test/output/tiles/1/0/0.png')
    assert not os.path.exists('test/output/tiles/1/1/0.png')
    assert not os.path.exists('test/output/tiles/1/2/1.png')
    assert os.path.exists('test/output/tiles/2/2/2.png')
    assert os.path.exists('test/output/tiles/2/2/3.png')
    assert os.path.exists('test/output/tiles/2/3/2.png')
    assert os.path.exists('test/output/tiles/2/3/3.png')
    assert not os.path.exists('test/output/tiles/2/2/1.png')
    assert not os.path.exists('test/output/tiles/2/1/1.png')
    assert not os.path.exists('test/output/tiles/2/0/0.png')
