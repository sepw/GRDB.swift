import XCTest
import GRDB

private protocol SchemaTraits {
    static var name: String { get }
}

private struct Record<Schema: SchemaTraits>: Equatable, Codable, TableRecord, FetchableRecord, PersistableRecord {
    static var databaseTableName: String { "\(Schema.name).test" }
    
    var id: Int
    var firstName: String
    var lastName: String
}

private struct TempDatabase: SchemaTraits {
    public static let name = "temp"
}

private struct AttachedDatabase: SchemaTraits {
    public static let name = "attached"
}

class QualifiedNameTests: GRDBTestCase {
    
    private func createTable<Schema>(in db: Database, schema: Schema.Type) throws where Schema: SchemaTraits {
        try db.create(table: "\(Schema.name).test") { t in
            t.column("id", .integer).primaryKey().notNull()
            t.column("firstName", .text).notNull()
            t.column("lastName", .text).notNull()
        }
    }
    
    private func createTableWithIndex<Schema>(in db: Database, schema: Schema.Type) throws where Schema: SchemaTraits {
        try db.create(table: "\(Schema.name).test") { t in
            t.column("id", .integer).primaryKey().notNull()
            t.column("firstName", .text).notNull().indexed()
            t.column("lastName", .text).notNull().indexed()
        }
    }

    private func attach(_ name: String, to db: Database) throws {
        try db.execute(sql: """
            ATTACH DATABASE ":memory:" AS "\(name)"
        """)
    }
    
    // MARK: -

    private func testInsertRecord<Schema>(into schema: Schema.Type, queue: DatabaseQueue) throws where Schema: SchemaTraits {
        try queue.write { db in
            try Record<Schema>(id: 1, firstName: "Herman", lastName: "Melville").insert(db)
            try Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller").insert(db)
        }

        let records = try queue.read { db in
            return try Record<Schema>.orderByPrimaryKey().fetchAll(db)
        }
        
        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0], Record<Schema>(id: 1, firstName: "Herman", lastName: "Melville"))
        XCTAssertEqual(records[1], Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller"))
    }
    
    private func testUpdateRecord<Schema>(in schema: Schema.Type, queue: DatabaseQueue) throws where Schema: SchemaTraits {
        try queue.write { db in
            try Record<Schema>(id: 1, firstName: "Herman", lastName: "Melville").insert(db)
            try Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller").insert(db)
        }

        try queue.write { db in
            var record = Record<Schema>(id: 1, firstName: "Herman", lastName: "Munster")
            try record.update(db)

            record = Record<Schema>(id: 2, firstName: "Arthur", lastName: "Morgan")
            try record.update(db)
        }
        
        let records = try queue.read { db in
            return try Record<Schema>.orderByPrimaryKey().fetchAll(db)
        }
        
        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0], Record(id: 1, firstName: "Herman", lastName: "Munster"))
        XCTAssertEqual(records[1], Record(id: 2, firstName: "Arthur", lastName: "Morgan"))
    }
    
    private func testDeleteRecord<Schema>(from schema: Schema.Type, queue: DatabaseQueue) throws where Schema: SchemaTraits {
        try queue.write { db in
            try Record<Schema>(id: 1, firstName: "Herman", lastName: "Melville").insert(db)
            try Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller").insert(db)
        }

        try queue.write { db in
            let _ = try Record<Schema>.deleteOne(db, key: 1)
        }
        
        let records = try queue.read { db in
            return try Record<Schema>.orderByPrimaryKey().fetchAll(db)
        }
        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records[0], Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller"))
    }
    
    private func testDeleteAllRecords<Schema>(from schema: Schema.Type, queue: DatabaseQueue) throws where Schema: SchemaTraits {
        try queue.write { db in
            try Record<Schema>(id: 1, firstName: "Herman", lastName: "Melville").insert(db)
            try Record<Schema>(id: 2, firstName: "Arthur", lastName: "Miller").insert(db)
        }
        
        try queue.write { db in
            let _ = try Record<Schema>.deleteAll(db)
        }

        let records = try queue.read { db in
            return try Record<Schema>.orderByPrimaryKey().fetchAll(db)
        }

        XCTAssertEqual(records.count, 0)
    }
    
    // MARK: - Temporary Database
    
    func testCreateTableInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
        }
    }
    
    func testCreateTableWithIndexInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTableWithIndex(in: db, schema: TempDatabase.self)
        }
    }

    func testTableExistsInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            let exists = try db.tableExists("\(TempDatabase.name).test")
            XCTAssertTrue(exists)
        }
    }
    
    func testPrimaryKeyInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            let keyInfo = try db.primaryKey("\(TempDatabase.name).test")
            XCTAssertNotNil(keyInfo)
        }
    }
    
    func testTableHasUniqueKeyInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            let unique = try db.table("\(TempDatabase.name).test", hasUniqueKey: [ "id" ])
            XCTAssertTrue(unique)
        }
    }
    
    func testIndexesInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTableWithIndex(in: db, schema: TempDatabase.self)
            let indexes = try db.indexes(on: "\(TempDatabase.name).test")
            XCTAssertEqual(indexes.count, 2)
        }
    }
    
    func testForeignKeysInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            let keys = try db.foreignKeys(on: "\(TempDatabase.name).test")
            XCTAssertEqual(keys.count, 0)
        }
    }
    
    func testDropTableInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.drop(table: "\(TempDatabase.name).test")
            XCTAssertFalse(try db.tableExists("\(TempDatabase.name).test"))
        }
    }
    
    func testAlterTableInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.alter(table: "\(TempDatabase.name).test") { a in
                a.add(column: "middleName", .text)
            }
        }
    }
    
    func testRenameTableInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.rename(table: "\(TempDatabase.name).test", to: "test_renamed")
            let exists = try db.tableExists("\(TempDatabase.name).test_renamed")
            XCTAssertTrue(exists)
        }
    }

    func testRenameTableToQualifiedNameInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.rename(table: "\(TempDatabase.name).test", to: "\(TempDatabase.name).test_renamed")
            let exists = try db.tableExists("\(TempDatabase.name).test_renamed")
            XCTAssertTrue(exists)
        }
    }

    func testCreateIndexInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.create(index: "\(TempDatabase.name).test_index", on: "test", columns: [ "firstName" ])
        }
    }

    func testCreateIndexInTempDatabaseOnTableWithQualifiedName() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.create(index: "\(TempDatabase.name).test_index", on: "\(TempDatabase.name).test", columns: [ "firstName" ])
        }
    }

    func testDropIndexInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
            try db.create(index: "\(TempDatabase.name).test_index", on: "test", columns: [ "firstName" ])
            try db.drop(index: "\(TempDatabase.name).test_index")
        }
    }

    // MARK: -

    func testInsertRecordIntoTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
        }
        try testInsertRecord(into: TempDatabase.self, queue: queue)
    }
    
    func testUpdateRecordInTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
        }
        try testUpdateRecord(in: TempDatabase.self, queue: queue)
    }
    
    func testDeleteRecordFromTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
        }
        try testDeleteRecord(from: TempDatabase.self, queue: queue)
    }
    
    func testDeleteAllRecordsFromTempDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try createTable(in: db, schema: TempDatabase.self)
        }
        try testDeleteAllRecords(from: TempDatabase.self, queue: queue)
    }
    
    // MARK: - Attached Database
    
    func testCreateTableInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
    }
    
    func testCreateTableWithIndexInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTableWithIndex(in: db, schema: AttachedDatabase.self)
        }
    }

    func testTableExistsInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            let exists = try db.tableExists("\(AttachedDatabase.name).test")
            XCTAssertTrue(exists)
        }
    }
    
    func testTableExistsForNonExistentTableInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            let exists = try db.tableExists("\(AttachedDatabase.name).non_existent_table")
            XCTAssertFalse(exists)
        }
    }
    
    func testPrimaryKeyInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            let keyInfo = try db.primaryKey("\(AttachedDatabase.name).test")
            XCTAssertNotNil(keyInfo)
        }
    }
    
    func testTableHasUniqueKeyInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            let unique = try db.table("\(AttachedDatabase.name).test", hasUniqueKey: [ "id" ])
            XCTAssertTrue(unique)
        }
    }
    
    func testIndexesInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTableWithIndex(in: db, schema: AttachedDatabase.self)
            let indexes = try db.indexes(on: "\(AttachedDatabase.name).test")
            XCTAssertEqual(indexes.count, 2)
        }
    }
    
    func testForeignKeysInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            let keys = try db.foreignKeys(on: "\(AttachedDatabase.name).test")
            XCTAssertEqual(keys.count, 0)
        }
    }
    
    func testDropTableInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.drop(table: "\(AttachedDatabase.name).test")
            XCTAssertFalse(try db.tableExists("\(AttachedDatabase.name).test"))
        }
    }
    
    func testAlterTableInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.alter(table: "\(AttachedDatabase.name).test") { a in
                a.add(column: "middleName", .text)
            }
        }
    }
    
    func testRenameTableInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.rename(table: "\(AttachedDatabase.name).test", to: "test_renamed")
            let exists = try db.tableExists("\(AttachedDatabase.name).test_renamed")
            XCTAssertTrue(exists)
        }
    }

    func testRenameTableToQualifiedNameInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.rename(table: "\(AttachedDatabase.name).test", to: "\(AttachedDatabase.name).test_renamed")
            let exists = try db.tableExists("\(AttachedDatabase.name).test_renamed")
            XCTAssertTrue(exists)
        }
    }

    func testCreateIndexInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.create(index: "\(AttachedDatabase.name).test_index", on: "test", columns: [ "firstName" ])
        }
    }

    func testCreateIndexInAttachedDatabaseOnTableWithQualifiedName() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.create(index: "\(AttachedDatabase.name).test_index", on: "\(AttachedDatabase.name).test", columns: [ "firstName" ])
        }
    }

    func testDropIndexInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
            try db.create(index: "\(AttachedDatabase.name).test_index", on: "test", columns: [ "firstName" ])
            try db.drop(index: "\(AttachedDatabase.name).test_index")
        }
    }

    // MARK: -

    func testInsertRecordIntoAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testInsertRecord(into: AttachedDatabase.self, queue: queue)
    }
    
    func testUpdateRecordInAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testUpdateRecord(in: AttachedDatabase.self, queue: queue)
    }
    
    func testDeleteRecordFromAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testDeleteRecord(from: AttachedDatabase.self, queue: queue)
    }
    
    func testDeleteAllRecordsFromAttachedDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testDeleteAllRecords(from: AttachedDatabase.self, queue: queue)
    }
    
    // MARK: - Disambiguation
    
    func testCreateTableInAmbiguousDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: TempDatabase.self)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
    }

    func testInsertRecordIntoAmbiguousDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: TempDatabase.self)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testInsertRecord(into: AttachedDatabase.self, queue: queue)
    }
    
    func testUpdateRecordInAmbiguousDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: TempDatabase.self)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testUpdateRecord(in: AttachedDatabase.self, queue: queue)
    }
    
    func testDeleteRecordFromAmbiguousDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: TempDatabase.self)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testDeleteRecord(from: AttachedDatabase.self, queue: queue)
    }
    
    func testDeleteAllRecordsFromAmbiguousDatabase() throws {
        let queue = DatabaseQueue()
        try queue.inDatabase { db in
            try attach(AttachedDatabase.name, to: db)
            try createTable(in: db, schema: TempDatabase.self)
            try createTable(in: db, schema: AttachedDatabase.self)
        }
        try testDeleteAllRecords(from: AttachedDatabase.self, queue: queue)
    }
}
