import sys, logging, re

from util import coordinate_to_tile, tile_to_coordinate, flip_y

logger = logging.getLogger(__name__)


def convert_tile_to_bbox(zoom, x, y, flip_tile_y):
    if flip_tile_y:
        y = flip_y(zoom, y)

    left, bottom = tile_to_coordinate(x - 0.5, y + 0.5, zoom)
    right, top   = tile_to_coordinate(x + 0.5, y - 0.5, zoom)

    sys.stdout.write("%f,%f,%f,%f\n" % (left, bottom, right, top))


def tiles_for_bbox(left, bottom, right, top, zoom, flip_tile_y):
    bottom_tile_x, bottom_tile_y = coordinate_to_tile(left, bottom, zoom)
    top_tile_x, top_tile_y       = coordinate_to_tile(right, top, zoom)

    if bottom_tile_y > top_tile_y:
        bottom_tile_y, top_tile_y = top_tile_y, bottom_tile_y

    for x in range(bottom_tile_x, top_tile_x+1):
        for y in range(bottom_tile_y, top_tile_y+1):
            if flip_tile_y:
                y = flip_y(zoom, y)
            sys.stdout.write("%d/%d/%d\n" % (zoom, x, y))


def convert_bbox_to_tiles(left, bottom, right, top, **kwargs):
    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    for z in range(min_zoom, max_zoom+1):
        tiles_for_bbox(left, bottom, right, top, z, kwargs.get('flip_y', False))


def convert_string(conversion_string, **kwargs):
	match = re.match(r'(\d+)/(\d+)/(\d+)', conversion_string, re.I)
	if match:
		convert_tile_to_bbox(int(match.group(1)), int(match.group(2)), int(match.group(3)), kwargs.get('flip_y', False))
		return

	match = re.match(r'([-0-9\.]+),([-0-9\.]+),([-0-9\.]+),([-0-9\.]+)', conversion_string, re.I)
	if match:
		convert_bbox_to_tiles(float(match.group(1)), float(match.group(2)), float(match.group(3)), float(match.group(4)), **kwargs)
