
def parentRecordCreate(engine, obj, when=None):
    when = when if (when is not None) else datetime.now() # to support testing
    as_dict = obj.to_dict(replace_uid=True)
    print("PARENT AS DICT", as_dict)
    with Session(engine) as session:
        parentDB = ParentDB(uid=obj.uid, data=as_dict, created_at=when, archived_at=None)
        session.add(parentDB)
        for child in obj.children:
            as_dict = child.to_dict()
            print("CHILD AS DICT", as_dict)
            childDB = ChildDB(uid=child.uid, data=as_dict, created_at=when, archived_at=None, parent_id=obj.uid)
            session.add(childDB)
        print("ABOUT TO COMMIT WHEN CREATING")
        session.commit()
        print("COMMITTED WHEN CREATING")

def parentRecordGet(engine, uid, cls, archived=False):
    with Session(engine) as session:
        query = session.query(ParentDB)\
                       .filter(ParentDB.uid == uid)
        if not archived:
            query = query.filter(ParentDB.archived_at == None)
        results = query.all()
        assert len(results) == 1
        return cls.from_dict(results[0].data)

def test_nested_record_persistence(engine, parent, first_child, second_child):
    parentRecordCreate(engine, parent)
    restored = parentRecordGet(engine, parent.uid, ParentEntity)
    assert restored == parent
