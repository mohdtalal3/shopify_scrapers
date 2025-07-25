from scrapers.lululemon.lululemon import complete_workflow_lululemon
from scrapers.sportsdirect.sportsdirect import complete_workflow_sportsdirect
from scrapers.mytheresa.mytheresa import complete_workflow_mytheresa
from scrapers.hypefly.hypefly import complete_workflow_hypefly
from scrapers.youngla.youngla import complete_workflow_youngla
from scrapers.tory.tory import complete_workflow_tory
from scrapers.thedesignerboxuk.thedesignerboxuk import complete_workflow_thedesignerboxuk
from scrapers.shop437.shop437 import complete_workflow_shop437
from scrapers.polene_paris.polene_paris import complete_workflow_polene_paris
from scrapers.Notorious.notorious import complete_workflow_notorious
from scrapers.gymshark.gymshark import complete_workflow_gymshark
from scrapers.aloyoga.aloyoga import complete_workflow_aloyoga
from color_maps import run_color_mapping




def run_all_scrapers():
    complete_workflow_lululemon()
    complete_workflow_sportsdirect()
    complete_workflow_mytheresa()
    complete_workflow_hypefly()
    complete_workflow_youngla()
    complete_workflow_tory()
    complete_workflow_thedesignerboxuk()
    complete_workflow_shop437()
    complete_workflow_polene_paris()
    complete_workflow_notorious()
    complete_workflow_gymshark()
    complete_workflow_aloyoga()
    run_color_mapping()