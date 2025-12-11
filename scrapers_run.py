import asyncio
import inspect
from scrapers.lululemon.lululemon import complete_workflow_lululemon
from scrapers.sportsdirect.sportsdirect import complete_workflow_sportsdirect
from scrapers.mytheresa.mytheresa import complete_workflow_mytheresa
from scrapers.hypefly.hypefly import complete_workflow_hypefly
from scrapers.youngla.youngla import complete_workflow_youngla
from scrapers.tory.tory import complete_workflow_tory
from scrapers.thedesignerboxuk.thedesignerboxuk import complete_workflow_thedesignerboxuk
from scrapers.shop437.shop437 import complete_workflow_437
from scrapers.polene_paris.polene_paris import complete_workflow_polene_paris
from scrapers.Notorious.notorious import complete_workflow_notorious
from scrapers.gymshark.gymshark import complete_workflow_gymshark
from scrapers.aloyoga.aloyoga import complete_workflow_aloyoga
from scrapers.araks.araks import complete_workflow_araks
from scrapers.balardi.balardi import complete_workflow_balardi
from scrapers.bandi.bandi import complete_workflow_bandit_running as complete_workflow_bandi
from scrapers.blssdfashion.blssdfashion import complete_workflow_blssdfashion
from scrapers.boohoo.boohoo import complete_workflow_boohoo
from scrapers.coach.coach import complete_workflow_coachoutlet
from scrapers.cocodemer.cocodemer import complete_workflow_fruitybooty as complete_workflow_cocodemer
from scrapers.cosabella.cosabella import complete_workflow_cosabella
from scrapers.cruise_fashion.cruise_fashion import complete_workflow_cruise_fashion
from scrapers.eberjey.eberjey import complete_workflow_eberjey
from scrapers.fruitybooty.fruitybooty import complete_workflow_fruitybooty
from scrapers.gemopticians.gemopticians import complete_workflow_gemopticians
from scrapers.hustle_culture.hustle_culture import complete_workflow_hustle_culture
from scrapers.karl.karl import complete_workflow_karl
from scrapers.kate.kate import complete_workflow_kate
from scrapers.katspade_outlet.kateoutlet import complete_workflow_kate_outlet
from scrapers.laperla.laperla import complete_workflow_laperla
from scrapers.livetheprocess.livetheprocess import complete_workflow_livetheprocess
from scrapers.lounge.lounge import complete_workflow_lounge
from scrapers.marcjacobs.marcjacobs import complete_workflow_marc_jacobs
from scrapers.meshki.meshki import complete_workflow_meshki
from scrapers.nakedwolfe.nakedwolf import complete_workflow_nakedwolf
from scrapers.oh_polly_uk.oh_polly_uk import complete_workflow_oh_polly_uk
from scrapers.organicbasics.organicbasics import complete_workflow_organicbasics
from scrapers.prettylittlething.prettylittlething import complete_workflow_pretty_little_things
from scrapers.ratandboa.ratandboa import complete_workflow_ratandboa
from scrapers.rhodeskin.rhodeskin import complete_workflow_polene_paris as complete_workflow_rhodeskin
from scrapers.riverisland.riverisland import complete_workflow_river
from scrapers.shop_whoop.shop_whoop import complete_workflow_shop_whoop
from scrapers.skims.skims import complete_workflow_skims
from scrapers.sportyandrich.sportyandrich import complete_workflow_sportyandrich
from scrapers.stanley.stanley import complete_workflow_stanley
from scrapers.superdown.superdown import complete_workflow_superdown
from scrapers.thereformation.thereformation import complete_workflow_thereformation
from scrapers.uk_polene.uk_polene import complete_workflow_uk_polene
from scrapers.underarmour.underarmour import complete_workflow_underarmour
from scrapers.vaara.vaara import complete_workflow_vaara
from scrapers.victoria.victoria import complete_workflow_victoria
try:
    from color_maps import run_color_mapping
except ImportError:
    print("Warning: color_maps module not found. Color mapping will be skipped")
    def run_color_mapping():
        print("Color mapping skipped - module not available")
        pass


def get_available_scrapers():
    """Return a dictionary of all available scrapers"""
    return {
        "coach_outlet": ("Coach Outlet", complete_workflow_coachoutlet),
        "lululemon": ("Lululemon", complete_workflow_lululemon),
        "sports_direct": ("Sports Direct", complete_workflow_sportsdirect),
        "mytheresa": ("Mytheresa", complete_workflow_mytheresa),
        "hypefly": ("Hypefly", complete_workflow_hypefly),
        "youngla": ("YoungLA", complete_workflow_youngla),
        "tory": ("Tory", complete_workflow_tory),
        "thedesignerboxuk": ("The Designer Box UK", complete_workflow_thedesignerboxuk),
        "shop437": ("Shop437", complete_workflow_437),
        "polene_paris": ("Polene Paris", complete_workflow_polene_paris),
        "notorious": ("Notorious", complete_workflow_notorious),
        "gymshark": ("Gymshark", complete_workflow_gymshark),
        "aloyoga": ("Alo Yoga", complete_workflow_aloyoga),
        "araks": ("Araks", complete_workflow_araks),
        "balardi": ("Balardi", complete_workflow_balardi),
        "bandi": ("Bandi", complete_workflow_bandi),
        "blssdfashion": ("BLSSD Fashion", complete_workflow_blssdfashion),
        "boohoo": ("Boohoo", complete_workflow_boohoo),
        "cocodemer": ("Cocodemer", complete_workflow_cocodemer),
        "cosabella": ("Cosabella", complete_workflow_cosabella),
        "cruise_fashion": ("Cruise Fashion", complete_workflow_cruise_fashion),
        "eberjey": ("Eberjey", complete_workflow_eberjey),
        "fruitybooty": ("Fruity Booty", complete_workflow_fruitybooty),
        "gemopticians": ("Gem Opticians", complete_workflow_gemopticians),
        "hustle_culture": ("Hustle Culture", complete_workflow_hustle_culture),
        "karl": ("Karl", complete_workflow_karl),
        "kate": ("Kate", complete_workflow_kate),
        "kate_outlet": ("Kate Spade Outlet", complete_workflow_kate_outlet),
        "laperla": ("La Perla", complete_workflow_laperla),
        "livetheprocess": ("Live The Process", complete_workflow_livetheprocess),
        "lounge": ("Lounge", complete_workflow_lounge),
        "marcjacobs": ("Marc Jacobs", complete_workflow_marc_jacobs),
        "meshki": ("Meshki", complete_workflow_meshki),
        "nakedwolfe": ("Naked Wolfe", complete_workflow_nakedwolf),
        "oh_polly_uk": ("Oh Polly UK", complete_workflow_oh_polly_uk),
        "organicbasics": ("Organic Basics", complete_workflow_organicbasics),
        "prettylittlething": ("Pretty Little Thing", complete_workflow_pretty_little_things),
        "ratandboa": ("Rat and Boa", complete_workflow_ratandboa),
        "rhodeskin": ("Rhode Skin", complete_workflow_rhodeskin),
        "riverisland": ("River Island", complete_workflow_river),
        "shop_whoop": ("Shop Whoop", complete_workflow_shop_whoop),
        "skims": ("Skims", complete_workflow_skims),
        "sportyandrich": ("Sporty and Rich", complete_workflow_sportyandrich),
        "stanley": ("Stanley", complete_workflow_stanley),
        "superdown": ("Superdown", complete_workflow_superdown),
        "thereformation": ("The Reformation", complete_workflow_thereformation),
        "uk_polene": ("UK Polene", complete_workflow_uk_polene),
        "underarmour": ("Under Armour", complete_workflow_underarmour),
        "vaara": ("Vaara", complete_workflow_vaara),
        "victoria": ("Victoria", complete_workflow_victoria)
    }


def run_selected_scrapers(scraper_ids=None, run_color_mapping_after=True):
    """
    Run specific scrapers by their IDs
    
    Args:
        scraper_ids: List of scraper IDs to run. If None or empty, runs all scrapers.
        run_color_mapping_after: Whether to run color mapping after scrapers complete
    
    Returns:
        dict: Results of scraper execution
    """
    available_scrapers = get_available_scrapers()
    
    # If no specific scrapers requested, run all
    if not scraper_ids:
        scraper_ids = list(available_scrapers.keys())
    
    results = {
        'completed': [],
        'failed': [],
        'total': len(scraper_ids)
    }
    
    print(f"\n🚀 Starting {len(scraper_ids)} scraper(s)...")
    
    for scraper_id in scraper_ids:
        if scraper_id not in available_scrapers:
            print(f"❌ Unknown scraper ID: {scraper_id}")
            results['failed'].append({
                'id': scraper_id,
                'name': scraper_id,
                'error': 'Unknown scraper ID'
            })
            continue
            
        scraper_name, scraper_function = available_scrapers[scraper_id]
        
        try:
            print(f"\n🔄 Starting {scraper_name} scraper...")
            # Check if the scraper function is async (coroutine)
            if inspect.iscoroutinefunction(scraper_function):
                asyncio.run(scraper_function())
            else:
                scraper_function()
            print(f"✅ {scraper_name} scraper completed successfully")
            results['completed'].append({
                'id': scraper_id,
                'name': scraper_name
            })
        except Exception as e:
            error_msg = str(e)
            print(f"❌ {scraper_name} scraper failed with error: {error_msg}")
            results['failed'].append({
                'id': scraper_id,
                'name': scraper_name,
                'error': error_msg
            })
            print(f"   Continuing with next scraper...")
    
    # Run color mapping if requested
    if run_color_mapping_after:
        try:
            print(f"\n🔄 Starting color mapping...")
            run_color_mapping()
            print(f"✅ Color mapping completed successfully")
            results['color_mapping'] = 'success'
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Color mapping failed with error: {error_msg}")
            results['color_mapping'] = f'failed: {error_msg}'
    
    return results


def run_all_scrapers():
    """Run all available scrapers (legacy function for backward compatibility)"""
    return run_selected_scrapers()


if __name__ == "__main__":
    run_all_scrapers()