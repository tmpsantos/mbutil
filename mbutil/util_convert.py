import sys, logging, re

from util import coordinate_to_tile, tile_to_coordinate, flip_y

logger = logging.getLogger(__name__)


def convert_tile_to_bbox(tile_z, tile_x, tile_y, flip_tile_y):
    if flip_tile_y:
        tile_y = flip_y(tile_z, tile_y)

    min_x, min_y = tile_to_coordinate(tile_x - 0.5, tile_y + 0.5, tile_z)
    max_x, max_y = tile_to_coordinate(tile_x + 0.5, tile_y - 0.5, tile_z)

    sys.stdout.write("%f,%f,%f,%f\n" % (min_x, min_y, max_x, max_y))


def tiles_for_bbox(left, bottom, right, top, tile_z, flip_tile_y):
    min_x, min_y = coordinate_to_tile(left, bottom, tile_z)
    max_x, max_y = coordinate_to_tile(right, top, tile_z)

    if min_y > max_y:
        min_y, max_y = max_y, min_y

    for tile_x in range(min_x, max_x+1):
        for tile_y in range(min_y, max_y+1):
            if flip_tile_y:
                tile_y = flip_y(tile_z, tile_y)
            sys.stdout.write("%d/%d/%d\n" % (tile_z, tile_x, tile_y))


def convert_bbox_to_tiles(left, bottom, right, top, **kwargs):
    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)
    flip_tile_y = kwargs.get('flip_y', False)

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    for tile_z in range(min_zoom, max_zoom+1):
        tiles_for_bbox(left, bottom, right, top, tile_z, flip_tile_y)


def convert_string(conversion_string, **kwargs):
	match = re.match(r'(\d+)/(\d+)/(\d+)', conversion_string, re.I)
	if match:
		convert_tile_to_bbox(int(match.group(1)), int(match.group(2)), int(match.group(3)), kwargs.get('flip_y', False))
		return

	match = re.match(r'([-0-9\.]+),([-0-9\.]+),([-0-9\.]+),([-0-9\.]+)', conversion_string, re.I)
	if match:
		convert_bbox_to_tiles(float(match.group(1)), float(match.group(2)), float(match.group(3)), float(match.group(4)), **kwargs)
