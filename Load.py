from sqlalchemy import create_engine,Table
import warnings
import configparser, os
import sys





warnings.filterwarnings('ignore', '^Unicode type received non-unicode bind param value')


if len(sys.argv)<2:
    print("Load.py destination")
    exit()


database=sys.argv[1]

if len(sys.argv)==3:
    language=sys.argv[2]
else:
    language='en'

fileLocation = os.path.dirname(os.path.realpath(__file__))
inifile=fileLocation+'/sdeloader.cfg'
config = configparser.ConfigParser()
config.read(inifile)
destination=config.get('Database',database)
sourcePath=config.get('Files','sourcePath')






from tableloader.tableFunctions import *



print("connecting to DB")


engine = create_engine(destination)
connection = engine.connect()



from tableloader.tables import metadataCreator

schema=None
if database=="postgresschema":
    schema="evesde"

metadata=metadataCreator(schema)



print("Creating Tables")

metadata.drop_all(engine,checkfirst=True)
metadata.create_all(engine,checkfirst=True)

print("created")

import tableloader.tableFunctions
types.import_types(connection,metadata,sourcePath,language)
types.import_bonus(connection,metadata,sourcePath,language)
types.import_materials(connection,metadata,sourcePath,language)
types.import_dogma(connection,metadata,sourcePath,language)
types.import_groups(connection,metadata,sourcePath,language)
types.import_categories(connection,metadata,sourcePath,language)
types.import_meta_groups(connection,metadata,sourcePath,language)
types.import_market_groups(connection,metadata,sourcePath,language)
typelist.import_type_lists(connection,metadata,sourcePath,language)
dogma.import_dogma_attribute_categories(connection,metadata,sourcePath,language)
dogma.import_dogma_attributes(connection,metadata,sourcePath,language)
dogma.import_dogma_effects(connection,metadata,sourcePath,language)
dogma.import_dogma_units(connection,metadata,sourcePath,language)
shipskills.buildSkills(connection, database)
blueprints.import_blueprints(connection,metadata,sourcePath,language)
npccorporations.import_corporation_activities(connection,metadata,sourcePath,language)
npccorporations.import_npc_corporation_divisions(connection,metadata,sourcePath,language)
npccorporations.import_npc_corporations(connection,metadata,sourcePath,language)
map.import_map(connection, metadata, sourcePath, language)
map.import_landmarks(connection, metadata, sourcePath, language)
map.import_npc_stations(connection, metadata, sourcePath, language)
map.import_station_services(connection, metadata, sourcePath, language)
planetary.import_planet_resources(connection, metadata, sourcePath, language)
planetary.import_planet_schematics(connection, metadata, sourcePath, language)
map.buildJumps(connection, database)
agents.import_agent_types(connection, metadata, sourcePath, language)
agents.import_agents_in_space(connection, metadata, sourcePath, language)
character.import_ancestries(connection, metadata, sourcePath, language)
character.import_bloodlines(connection, metadata, sourcePath, language)
character.import_character_attributes(connection, metadata, sourcePath, language)
character.import_factions(connection, metadata, sourcePath, language)
character.import_races(connection, metadata, sourcePath, language)
npccharacters.import_npc_characters(connection, metadata, sourcePath, language)
certificates.import_certificates(connection, metadata, sourcePath, language)
certificates.import_masteries(connection, metadata, sourcePath, language)
skins.import_skin_materials(connection, metadata, sourcePath, language)
skins.import_skins(connection, metadata, sourcePath, language)
skins.import_skin_licenses(connection, metadata, sourcePath, language)
graphics.import_graphics(connection, metadata, sourcePath, language)
graphics.import_icons(connection, metadata, sourcePath, language)
invNames.build_inv_names(connection, database)
cloneGrades.import_clone_grades(connection, metadata, sourcePath, language)
