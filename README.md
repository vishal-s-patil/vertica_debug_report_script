queries.json file 
    if past of a perticular query does not exists add query_past = select null
    if present of a perticular query does not exists add query = select null
    if past and present of a perticular query are different add both query and query_past
    if both past and present of a perticular query are same, them add query and do not add query_past field.

--duration:
    unit of duration is hours, you can pass decimal values also to check only for new mins
    ex. for 30 min you can pass --duation=0.5; default 3 hours.