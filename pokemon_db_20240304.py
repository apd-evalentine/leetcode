"""
LeetCode question 20240304
Pokemon Database
"""

import json
import requests
import polars as pl
from PIL import Image
from pathlib import Path

from typing import Dict, Union

"""
I was going to build this into snowflake w/ snowpark 
but then realized that it would waste snowflake compute for a simple leet code project and other people might already be doing that
Since not many people use polars and it is my favorite library that is sweeping the dataframe world by storm I will leverage it here
"""

# build empty polars table
t_schema = {
    'id': pl.Int16,
    'name': pl.String,
    'url': pl.String
}

pokemon_url = 'https://pokeapi.co/api/v2/pokemon/?offset=0&limit=1302'

loc = Path(__file__).parent

def build_pokemon_tbl(url: str, tbl_schema: Dict) -> pl.DataFrame:

    tbl = pl.DataFrame(
        schema=tbl_schema
    )

    response = requests.get(url)
    results = json.loads(response.text)['results']
    results_df = pl.DataFrame(results)
    results_df = (
        results_df
        .with_columns(
            pl.col('url').str.extract(r".*/(\d+)/$").cast(tbl_schema['id']).alias('id')
        )
        # to perform concat cols need to be in same order
        .select(list(tbl_schema))
    )
    tbl = pl.concat([tbl, results_df])
    return tbl

def get_sprite_url(url: str) -> str:
    response = requests.get(url)
    sprites_dict = json.loads(response.text)['sprites']
    first_key = next(iter(sprites_dict))
    return sprites_dict[first_key]

def add_sprite_col(tbl: pl.DataFrame, ref_col: str) -> pl.DataFrame:
    tbl = tbl.with_columns(
        pl.col(ref_col).map_elements(
            get_sprite_url,
            return_dtype=pl.String, 
            strategy='threading',
        ).alias('sprite')
    )
    return tbl

def get_pokemon_image(
        lookup: Union[int, str], 
        tbl: pl.DataFrame = None,
        save_image: bool = True
    ) -> Image:
    lookup_col = 'id'
    if isinstance(lookup, str):
        lookup_col = 'name'
    if tbl is None:
        tbl = pokemon_tbl
    sprite_url = (
        pokemon_tbl
        .filter(pl.col(lookup_col)==lookup)
        .select(pl.col('sprite'))[0,0]
    )
    response = requests.get(sprite_url, stream=True)
    img = Image.open(response.raw).convert()
    if save_image:
        save_path = str(Path(__file__).parent) + '\pokemon_image.png'
        img.save(save_path)
    return img.show()

pokemon_tbl = build_pokemon_tbl(pokemon_url, t_schema)

pokemon_tbl = add_sprite_col(pokemon_tbl, 'url')
print("First 10 Records of Table")
print(pokemon_tbl.head(10))
print("Last 10 Records of Table")
print(pokemon_tbl.tail(10))
print("Displaying Lookup Image")
get_pokemon_image(lookup='charmander')
