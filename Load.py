from sqlalchemy import create_engine, Table, delete
import warnings
import configparser, os
import sys



warnings.filterwarnings('ignore', '^Unicode type received non-unicode bind param value')


if len(sys.argv) < 2:
    print("Load.py destination [language]")
    sys.exit(1)

database = sys.argv[1]
language = sys.argv[2] if len(sys.argv) == 3 else 'en'

fileLocation = os.path.dirname(os.path.realpath(__file__))
inifile = fileLocation + '/sdeloader.cfg'
config = configparser.ConfigParser()
config.read(inifile)
destination = config.get('Database', database)
sourcePath = config.get('Files', 'sourcePath')


from tableloader.tableFunctions import *
import tableloader.tableFunctions

print("connecting to DB")

engine = create_engine(destination)
connection = engine.connect()

from tableloader.tables import metadataCreator

schema = None
if database == "postgresschema":
    schema = "evesde"

metadata = metadataCreator(schema)

# ---------------------------------------------------------------------------
# Detect update mode via environment variable (set by run-conversion.sh)
# ---------------------------------------------------------------------------
_raw_changed = os.environ.get('SDE_CHANGED_FILES', '')
_changed_files = {f.strip().lower() for f in _raw_changed.splitlines() if f.strip()}
update_mode = bool(_changed_files)

if update_mode:
    print(f"Update mode: {len(_changed_files)} changed file(s)")
else:
    print("Full mode: dropping and recreating all tables")

# ---------------------------------------------------------------------------
# LOADERS — defines what each module watches, owns, and does
#
# fields:
#   module_name           : str  — slug used in depends_on_modules references
#   files                 : list[str]  — JSONL basenames (case-insensitive) that trigger this loader
#   tables                : list[str]  — DB table names owned by this loader (drop/recreate in update mode)
#   calls                 : list[callable]  — functions to invoke (no args; closures over runtime vars)
#   trnTranslations_tcIDs : list[int]  (optional) — in update mode, DELETE these tcIDs before reload
#   depends_on_modules    : list[str]  (optional) — for derived loaders; triggered if any named module ran
# ---------------------------------------------------------------------------
LOADERS = [
    {
        'module_name': 'types',
        'files': ['types.jsonl'],
        'tables': ['invTypes', 'invMetaTypes'],
        'trnTranslations_tcIDs': [8, 33],
        'calls': [
            lambda: types.import_types(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'typebonus',
        'files': ['typebonus.jsonl'],
        'tables': ['invTraits'],
        'calls': [
            lambda: types.import_bonus(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'typematerials',
        'files': ['typematerials.jsonl'],
        'tables': ['invTypeMaterials'],
        'calls': [
            lambda: types.import_materials(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'typedogma',
        'files': ['typedogma.jsonl'],
        'tables': ['dgmTypeAttributes', 'dgmTypeEffects'],
        'calls': [
            lambda: types.import_dogma(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'groups',
        'files': ['groups.jsonl'],
        'tables': ['invGroups'],
        'trnTranslations_tcIDs': [7],
        'calls': [
            lambda: types.import_groups(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'categories',
        'files': ['categories.jsonl'],
        'tables': ['invCategories'],
        'trnTranslations_tcIDs': [6],
        'calls': [
            lambda: types.import_categories(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'metagroups',
        'files': ['metagroups.jsonl'],
        'tables': ['invMetaGroups'],
        'trnTranslations_tcIDs': [15],
        'calls': [
            lambda: types.import_meta_groups(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'marketgroups',
        'files': ['marketgroups.jsonl'],
        'tables': ['invMarketGroups'],
        'trnTranslations_tcIDs': [14],
        'calls': [
            lambda: types.import_market_groups(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'typelist',
        'files': ['typelists.jsonl'],
        'tables': [
            'typeListsHeader', 'typeListsIncludedTypeIDs', 'typeListsExcludedTypeIDs',
            'typeListsIncludedGroupIDs', 'typeListsExcludedGroupIDs',
            'typeListsIncludedCategoryIDs', 'typeListsExcludedCategoryIDs',
        ],
        'calls': [
            lambda: typelist.import_type_lists(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'dogmaattributecategories',
        'files': ['dogmaattributecategories.jsonl'],
        'tables': ['dgmAttributeCategories'],
        'calls': [
            lambda: dogma.import_dogma_attribute_categories(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'dogmaattributes',
        'files': ['dogmaattributes.jsonl'],
        'tables': ['dgmAttributeTypes'],
        'calls': [
            lambda: dogma.import_dogma_attributes(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'dogmaeffects',
        'files': ['dogmaeffects.jsonl'],
        'tables': ['dgmEffects'],
        'calls': [
            lambda: dogma.import_dogma_effects(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'dogmaunits',
        'files': ['dogmaunits.jsonl'],
        'tables': ['eveUnits'],
        'calls': [
            lambda: dogma.import_dogma_units(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'shipskills',
        'files': [],
        'tables': ['shipSkills'],
        'depends_on_modules': ['types', 'typedogma', 'dogmaattributecategories', 'dogmaattributes', 'dogmaeffects', 'dogmaunits'],
        'calls': [
            lambda: shipskills.buildSkills(connection, database),
        ],
    },
    {
        'module_name': 'blueprints',
        'files': ['blueprints.jsonl'],
        'tables': [
            'industryBlueprints', 'industryActivity', 'industryActivityMaterials',
            'industryActivityProducts', 'industryActivityProbabilities', 'industryActivitySkills',
        ],
        'calls': [
            lambda: blueprints.import_blueprints(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'corporationactivities',
        'files': ['corporationactivities.jsonl'],
        'tables': ['crpActivities'],
        'calls': [
            lambda: npccorporations.import_corporation_activities(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'npccorporationdivisions',
        'files': ['npccorporationdivisions.jsonl'],
        'tables': ['crpNPCDivisions'],
        'trnTranslations_tcIDs': [24],
        'calls': [
            lambda: npccorporations.import_npc_corporation_divisions(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'npccorporations',
        'files': ['npccorporations.jsonl'],
        'tables': ['crpNPCCorporations', 'crpNPCCorporationDivisions', 'crpNPCCorporationTrades'],
        'trnTranslations_tcIDs': [20],
        'calls': [
            lambda: npccorporations.import_npc_corporations(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'map',
        'files': [
            'mapregions.jsonl', 'mapconstellations.jsonl', 'mapsolarsystems.jsonl',
            'mapstars.jsonl', 'mapplanets.jsonl', 'mapmoons.jsonl',
            'mapasteroidbelts.jsonl', 'mapstargates.jsonl', 'mapsecondarysuns.jsonl',
            'npcstations.jsonl', 'stationservices.jsonl', 'landmarks.jsonl',
        ],
        'tables': [
            'mapRegions', 'mapConstellations', 'mapSolarSystems', 'mapDenormalize',
            'mapCelestialStatistics', 'mapCelestialGraphics', 'mapJumps',
            'mapLocationWormholeClasses', 'mapLandmarks', 'staStations', 'staServices',
        ],
        'calls': [
            lambda: map.import_map(connection, metadata, sourcePath, language),
            lambda: map.import_landmarks(connection, metadata, sourcePath, language),
            lambda: map.import_npc_stations(connection, metadata, sourcePath, language),
            lambda: map.import_station_services(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'planetary',
        'files': ['planetresources.jsonl', 'planetschematics.jsonl'],
        'tables': [
            'planetResources', 'planetSchematics', 'planetSchematicsPinMap', 'planetSchematicsTypeMap',
        ],
        'calls': [
            lambda: planetary.import_planet_resources(connection, metadata, sourcePath, language),
            lambda: planetary.import_planet_schematics(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'buildjumps',
        'files': [],
        'tables': ['mapSolarSystemJumps', 'mapRegionJumps', 'mapConstellationJumps'],
        'depends_on_modules': ['map'],
        'calls': [
            lambda: map.buildJumps(connection, database),
        ],
    },
    {
        'module_name': 'agents',
        'files': ['agenttypes.jsonl', 'agentsinspace.jsonl'],
        'tables': ['agtAgentTypes', 'agtAgentsInSpace'],
        'calls': [
            lambda: agents.import_agent_types(connection, metadata, sourcePath, language),
            lambda: agents.import_agents_in_space(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'ancestries',
        'files': ['ancestries.jsonl'],
        'tables': ['chrAncestries'],
        'trnTranslations_tcIDs': [12],
        'calls': [
            lambda: character.import_ancestries(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'bloodlines',
        'files': ['bloodlines.jsonl'],
        'tables': ['chrBloodlines'],
        'trnTranslations_tcIDs': [11],
        'calls': [
            lambda: character.import_bloodlines(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'characterattributes',
        'files': ['characterattributes.jsonl'],
        'tables': ['chrAttributes'],
        'calls': [
            lambda: character.import_character_attributes(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'factions',
        'files': ['factions.jsonl'],
        'tables': ['chrFactions'],
        'trnTranslations_tcIDs': [19],
        'calls': [
            lambda: character.import_factions(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'races',
        'files': ['races.jsonl'],
        'tables': ['chrRaces'],
        'trnTranslations_tcIDs': [16],
        'calls': [
            lambda: character.import_races(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'npccharacters',
        'files': ['npccharacters.jsonl'],
        'tables': ['npcCharacters', 'agtAgents', 'agtResearchAgents'],
        'calls': [
            lambda: npccharacters.import_npc_characters(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'certificates',
        'files': ['certificates.jsonl'],
        'tables': ['certCerts', 'certSkills'],
        'trnTranslations_tcIDs': [17],
        'calls': [
            lambda: certificates.import_certificates(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'masteries',
        'files': ['masteries.jsonl'],
        'tables': ['certMasteries'],
        'calls': [
            lambda: certificates.import_masteries(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinmaterials',
        'files': ['skinmaterials.jsonl'],
        'tables': ['skinMaterials'],
        'calls': [
            lambda: skins.import_skin_materials(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skins',
        'files': ['skins.jsonl'],
        'tables': ['skins', 'skinShip'],
        'calls': [
            lambda: skins.import_skins(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinlicenses',
        'files': ['skinlicenses.jsonl'],
        'tables': ['skinLicense'],
        'calls': [
            lambda: skins.import_skin_licenses(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'graphics',
        'files': ['graphicmaterialsets.jsonl', 'graphics.jsonl', 'icons.jsonl'],
        'tables': ['graphicMaterialSets', 'eveGraphics', 'eveIcons'],
        'calls': [
            lambda: graphics.import_graphic_material_sets(connection, metadata, sourcePath, language),
            lambda: graphics.import_graphics(connection, metadata, sourcePath, language),
            lambda: graphics.import_icons(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'invnames',
        'files': [],
        'tables': ['invNames', 'invUniqueNames'],
        'depends_on_modules': ['map', 'types', 'npccorporations', 'npccharacters'],
        'calls': [
            lambda: invNames.build_inv_names(connection, database),
        ],
    },
    {
        'module_name': 'clonegrades',
        'files': ['clonegrades.jsonl'],
        'tables': ['chrCloneGrades', 'chrCloneGradeSkills'],
        'calls': [
            lambda: cloneGrades.import_clone_grades(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'charactertitles',
        'files': ['charactertitles.jsonl'],
        'tables': ['chrTitles'],
        'calls': [
            lambda: characterTitles.import_character_titles(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'archetypes',
        'files': ['archetypes.jsonl'],
        'tables': ['dungeonArchetypes'],
        'calls': [
            lambda: missions.import_archetypes(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'missions',
        'files': ['missions.jsonl'],
        'tables': ['mstMissions', 'mstMissionMessages', 'mstMissionExtraStandings'],
        'calls': [
            lambda: missions.import_missions(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'epicarcs',
        'files': ['epicarcs.jsonl'],
        'tables': ['epicArcs', 'epicArcMissions', 'epicArcMissionNextMissions'],
        'calls': [
            lambda: missions.import_epic_arcs(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'dungeons',
        'files': ['dungeons.jsonl'],
        'tables': ['dungeons', 'dungeonAllowedShips'],
        'calls': [
            lambda: missions.import_dungeons(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'shiptreeelements',
        'files': ['shiptreeelements.jsonl'],
        'tables': ['shipTreeElements'],
        'calls': [
            lambda: shiptree.import_ship_tree_elements(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'shiptreegroups',
        'files': ['shiptreegroups.jsonl'],
        'tables': ['shipTreeGroups', 'shipTreeGroupElements', 'shipTreeGroupPreReqSkills'],
        'calls': [
            lambda: shiptree.import_ship_tree_groups(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'shiptreefactions',
        'files': ['shiptreefactions.jsonl'],
        'tables': ['shipTreeFactions', 'shipTreeFactionElements'],
        'calls': [
            lambda: shiptree.import_ship_tree_factions(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'typeelements',
        'files': ['typeelements.jsonl'],
        'tables': ['typeElements'],
        'calls': [
            lambda: shiptree.import_type_elements(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'military',
        'files': ['militarycampaigns.jsonl', 'militarycampaignobjectives.jsonl'],
        'tables': ['milCampaigns', 'milCampaignObjectives', 'milCampaignObjContentTags'],
        'calls': [
            lambda: military.import_military_campaigns(connection, metadata, sourcePath, language),
            lambda: military.import_military_campaign_objectives(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'mercenary',
        'files': ['mercenarytacticaloperations.jsonl'],
        'tables': ['mercenaryTacticalOperations'],
        'calls': [
            lambda: military.import_mercenary_tactical_operations(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'sovereigntyupgrades',
        'files': ['sovereigntyupgrades.jsonl'],
        'tables': ['sovereigntyUpgrades'],
        'calls': [
            lambda: military.import_sovereignty_upgrades(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrcomponentcategories',
        'files': ['skinrcomponentcategories.jsonl'],
        'tables': ['skinrComponentCategories'],
        'calls': [
            lambda: skinr.import_skinr_component_categories(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrcomponentrarities',
        'files': ['skinrcomponentrarities.jsonl'],
        'tables': ['skinrComponentRarities'],
        'calls': [
            lambda: skinr.import_skinr_component_rarities(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrcomponentpointvalues',
        'files': ['skinrcomponentpointvalues.jsonl'],
        'tables': ['skinrComponentPointValues'],
        'calls': [
            lambda: skinr.import_skinr_component_point_values(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrcomponents',
        'files': ['skinrcomponents.jsonl'],
        'tables': ['skinrComponents', 'skinrComponentTypes'],
        'calls': [
            lambda: skinr.import_skinr_components(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrslotcategories',
        'files': ['skinrslotcategories.jsonl'],
        'tables': ['skinrSlotCategories'],
        'calls': [
            lambda: skinr.import_skinr_slot_categories(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrslotnames',
        'files': ['skinrslotnames.jsonl'],
        'tables': ['skinrSlotNames'],
        'calls': [
            lambda: skinr.import_skinr_slot_names(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrslots',
        'files': ['skinrslots.jsonl'],
        'tables': ['skinrSlots', 'skinrSlotAllowedCategories'],
        'calls': [
            lambda: skinr.import_skinr_slots(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrslotconfigurations',
        'files': ['skinrslotconfigurations.jsonl'],
        'tables': ['skinrSlotConfigurations', 'skinrSlotConfigurationSlots', 'skinrSlotConfigurationShips'],
        'calls': [
            lambda: skinr.import_skinr_slot_configurations(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'skinrtierthresholds',
        'files': ['skinrtierthresholds.jsonl'],
        'tables': ['skinrTierThresholds'],
        'calls': [
            lambda: skinr.import_skinr_tier_thresholds(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'compressible',
        'files': ['compressibletypes.jsonl'],
        'tables': ['compressibleTypes'],
        'calls': [
            lambda: compressible.import_compressible_types(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'freelance',
        'files': ['freelancejobschemas.jsonl'],
        'tables': ['freelanceJobSchemas', 'freelanceJobSchemaContentTags', 'freelanceJobSchemaParameters'],
        'calls': [
            lambda: freelance.import_freelance_job_schemas(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'translationlanguages',
        'files': ['translationlanguages.jsonl'],
        'tables': ['trnTranslationLanguages'],
        'calls': [
            lambda: translationlanguages.import_translation_languages(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'contraband',
        'files': ['contrabandtypes.jsonl'],
        'tables': ['invContrabandTypes'],
        'calls': [
            lambda: contraband.import_contraband_types(connection, metadata, sourcePath, language),
        ],
    },
    {
        'module_name': 'controltower',
        'files': ['controltowerresources.jsonl'],
        'tables': ['invControlTowerResources'],
        'calls': [
            lambda: controltower.import_control_tower_resources(connection, metadata, sourcePath, language),
        ],
    },
]


def _resolve_tables(table_names):
    result = []
    for name in table_names:
        tbl = metadata.tables.get(name)
        if tbl is None and schema:
            tbl = metadata.tables.get(f"{schema}.{name}")
        if tbl is not None:
            result.append(tbl)
    return result


def _run_full():
    print("Creating Tables")
    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine, checkfirst=True)
    print("created")

    for loader in LOADERS:
        for call in loader['calls']:
            call()


def _run_update():
    trn_table = metadata.tables.get('trnTranslations')
    if trn_table is None and schema:
        trn_table = metadata.tables.get(f"{schema}.trnTranslations")
    ran_modules = set()

    for loader in LOADERS:
        triggered = any(f.lower() in _changed_files for f in loader['files'])
        if not triggered and loader.get('depends_on_modules'):
            triggered = bool(ran_modules & set(loader['depends_on_modules']))
        if not triggered:
            continue

        print(f"Update: reloading module '{loader['module_name']}'")

        if trn_table is not None and loader.get('trnTranslations_tcIDs'):
            tcids = loader['trnTranslations_tcIDs']
            connection.execute(delete(trn_table).where(trn_table.c.tcID.in_(tcids)))
            # SQLAlchemy 2.0 autobegins a transaction on execute(); commit it now
            # so the loader's own connection.begin() does not raise InvalidRequestError.
            # The DELETE and the subsequent INSERTs are therefore in separate transactions;
            # a loader crash after this point requires a full reload to restore these rows.
            connection.commit()

        affected = _resolve_tables(loader['tables'])
        if affected:
            metadata.drop_all(engine, tables=affected, checkfirst=True)
            metadata.create_all(engine, tables=affected, checkfirst=True)

        for call in loader['calls']:
            call()

        ran_modules.add(loader['module_name'])

    if not ran_modules:
        print("Update mode: no loaders were triggered — nothing to do")
    else:
        print(f"Update mode complete: ran {len(ran_modules)} module(s): {', '.join(sorted(ran_modules))}")


if update_mode:
    _run_update()
else:
    _run_full()
