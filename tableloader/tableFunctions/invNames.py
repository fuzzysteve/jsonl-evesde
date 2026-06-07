# -*- coding: utf-8 -*-

from sqlalchemy import Table, select, insert, text


def build_inv_names(connection, connectiontype):
    """
    Populates invNames and invUniqueNames from mapDenormalize.

    invNames      — one row per itemID that has a non-null name
    invUniqueNames — same, but itemName must be unique; duplicates are skipped

    Call this after all map data and station names are loaded.
    """
    print("Building invNames and invUniqueNames from mapDenormalize")

    sql = {}

    # Sources for invNames:
    #   mapDenormalize  — all universe objects (regions, systems, stars, planets,
    #                     moons, belts, stargates, stations)
    #   invTypes        — all type names            (groupID from invTypes)
    #   crpNPCCorporations — NPC corp names         (groupID 2)
    #   agtNpcCharacters   — NPC character names    (groupID 1)

    sql['postgres'] = [
        """INSERT INTO "invNames" ("itemID", "itemName")
           SELECT "itemID", "itemName" FROM "mapDenormalize" WHERE "itemName" IS NOT NULL
           UNION ALL
           SELECT "typeID", "typeName" FROM "invTypes" WHERE "typeName" IS NOT NULL
           UNION ALL
           SELECT "corporationID", "corporationName" FROM "crpNPCCorporations" WHERE "corporationName" IS NOT NULL
           UNION ALL
           SELECT "characterID", "characterName" FROM "npcCharacters" WHERE "characterName" IS NOT NULL""",

        """INSERT INTO "invUniqueNames" ("itemID", "itemName", "groupID")
           SELECT src."itemID", src."itemName", src."groupID" FROM (
               SELECT "itemID", "itemName", "groupID" FROM "mapDenormalize" WHERE "itemName" IS NOT NULL
               UNION ALL
               SELECT t."typeID", t."typeName", t."groupID" FROM "invTypes" t WHERE t."typeName" IS NOT NULL
               UNION ALL
               SELECT "corporationID", "corporationName", 2 FROM "crpNPCCorporations" WHERE "corporationName" IS NOT NULL
               UNION ALL
               SELECT "characterID", "characterName", 1 FROM "npcCharacters" WHERE "characterName" IS NOT NULL
           ) src
           WHERE src."itemName" NOT IN (
               SELECT "itemName" FROM (
                   SELECT "itemName" FROM "mapDenormalize" WHERE "itemName" IS NOT NULL
                   UNION ALL
                   SELECT "typeName" FROM "invTypes" WHERE "typeName" IS NOT NULL
                   UNION ALL
                   SELECT "corporationName" FROM "crpNPCCorporations" WHERE "corporationName" IS NOT NULL
                   UNION ALL
                   SELECT "characterName" FROM "npcCharacters" WHERE "characterName" IS NOT NULL
               ) all_names
               GROUP BY "itemName" HAVING COUNT(*) > 1
           )""",
    ]

    sql['postgresschema'] = [
        """INSERT INTO evesde."invNames" ("itemID", "itemName")
           SELECT "itemID", "itemName" FROM evesde."mapDenormalize" WHERE "itemName" IS NOT NULL
           UNION ALL
           SELECT "typeID", "typeName" FROM evesde."invTypes" WHERE "typeName" IS NOT NULL
           UNION ALL
           SELECT "corporationID", "corporationName" FROM evesde."crpNPCCorporations" WHERE "corporationName" IS NOT NULL
           UNION ALL
           SELECT "characterID", "characterName" FROM evesde."npcCharacters" WHERE "characterName" IS NOT NULL""",

        """INSERT INTO evesde."invUniqueNames" ("itemID", "itemName", "groupID")
           SELECT src."itemID", src."itemName", src."groupID" FROM (
               SELECT "itemID", "itemName", "groupID" FROM evesde."mapDenormalize" WHERE "itemName" IS NOT NULL
               UNION ALL
               SELECT t."typeID", t."typeName", t."groupID" FROM evesde."invTypes" t WHERE t."typeName" IS NOT NULL
               UNION ALL
               SELECT "corporationID", "corporationName", 2 FROM evesde."crpNPCCorporations" WHERE "corporationName" IS NOT NULL
               UNION ALL
               SELECT "characterID", "characterName", 1 FROM evesde."npcCharacters" WHERE "characterName" IS NOT NULL
           ) src
           WHERE src."itemName" NOT IN (
               SELECT "itemName" FROM (
                   SELECT "itemName" FROM evesde."mapDenormalize" WHERE "itemName" IS NOT NULL
                   UNION ALL
                   SELECT "typeName" FROM evesde."invTypes" WHERE "typeName" IS NOT NULL
                   UNION ALL
                   SELECT "corporationName" FROM evesde."crpNPCCorporations" WHERE "corporationName" IS NOT NULL
                   UNION ALL
                   SELECT "characterName" FROM evesde."npcCharacters" WHERE "characterName" IS NOT NULL
               ) all_names
               GROUP BY "itemName" HAVING COUNT(*) > 1
           )""",
    ]

    sql['other'] = [
        """INSERT INTO invNames (itemID, itemName)
           SELECT itemID, itemName FROM mapDenormalize WHERE itemName IS NOT NULL
           UNION ALL
           SELECT typeID, typeName FROM invTypes WHERE typeName IS NOT NULL
           UNION ALL
           SELECT corporationID, corporationName FROM crpNPCCorporations WHERE corporationName IS NOT NULL
           UNION ALL
           SELECT characterID, characterName FROM npcCharacters WHERE characterName IS NOT NULL""",

        """INSERT INTO invUniqueNames (itemID, itemName, groupID)
           SELECT src.itemID, src.itemName, src.groupID FROM (
               SELECT itemID, itemName, groupID FROM mapDenormalize WHERE itemName IS NOT NULL
               UNION ALL
               SELECT t.typeID, t.typeName, t.groupID FROM invTypes t WHERE t.typeName IS NOT NULL
               UNION ALL
               SELECT corporationID, corporationName, 2 FROM crpNPCCorporations WHERE corporationName IS NOT NULL
               UNION ALL
               SELECT characterID, characterName, 1 FROM npcCharacters WHERE characterName IS NOT NULL
           ) src
           WHERE src.itemName NOT IN (
               SELECT itemName FROM (
                   SELECT itemName FROM mapDenormalize WHERE itemName IS NOT NULL
                   UNION ALL
                   SELECT typeName FROM invTypes WHERE typeName IS NOT NULL
                   UNION ALL
                   SELECT corporationName FROM crpNPCCorporations WHERE corporationName IS NOT NULL
                   UNION ALL
                   SELECT characterName FROM npcCharacters WHERE characterName IS NOT NULL
               ) all_names
               GROUP BY itemName HAVING COUNT(*) > 1
           )""",
    ]

    if connectiontype in ('sqlite', 'mysql', 'mssql'):
        connectiontype = 'other'

    trans = connection.begin()
    for statement in sql[connectiontype]:
        connection.execute(text(statement))
    trans.commit()
    print("invNames and invUniqueNames built")
