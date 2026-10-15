from types import SimpleNamespace as NS
import pytest
from okto_nexus_connector.cli.commands.workspace_selection import select_workspace
from okto_nexus_connector.errors import ConnectorError


@pytest.mark.parametrize('selected', ['ws_remote', None])
async def test_select_existing_or_new_workspace_with_same_display_names(selected):
    pages = [dict(items=[dict(workspace_id='ws_local',display_name='Project')],has_more=True,next_workspace_id='ws_local'),
             dict(items=[dict(workspace_id='ws_remote',display_name='Project')],has_more=False,next_workspace_id=None)]
    calls=[]
    async def listing(key,**kw):
        assert key=='key';calls.append(kw);return pages[len(calls)-1]
    def choose(label,options,default):
        assert options==[('ws_local','Project (ws_local)'),('ws_remote','Project (ws_remote)'),(None,'Create new workspace')]
        return selected
    result=await select_workspace(NS(workspace_id=None,workspace_label='New'),NS(line=lambda _:None),
        NS(list_connection_workspaces=listing),'key',choice=choose,text=lambda *a,**k:pytest.fail('unexpected prompt'))
    assert result==(selected,'Project' if selected else 'New')
    assert calls==[{'after_workspace_id':None},{'after_workspace_id':'ws_local'}]


async def test_explicit_id_does_not_require_catalog_permission():
    result=await select_workspace(NS(workspace_id='ws_existing',workspace_label='Known'),None,None,None,
        choice=lambda *a:pytest.fail('unexpected prompt'),text=None)
    assert result==('ws_existing','Known')


@pytest.mark.parametrize('code',['PERMISSION_DENIED','NOT_FOUND'])
async def test_unavailable_catalog_offers_explicit_id_without_creating_silently(code):
    async def listing(*a,**kw):raise ConnectorError(code,'catalog','Unavailable')
    messages=[]
    result=await select_workspace(NS(workspace_id=None,workspace_label=None),NS(line=messages.append),
        NS(list_connection_workspaces=listing),'key',choice=lambda *a:True,text=lambda *a,**k:'ws_known')
    assert result==('ws_known',None) and messages


async def test_network_failure_never_silently_creates_new_workspace():
    async def listing(*a,**kw):raise ConnectorError('CONTROL_DISCONNECTED','catalog','Offline')
    with pytest.raises(ConnectorError,match='Offline'):
        await select_workspace(NS(workspace_id=None,workspace_label=None),NS(line=lambda _:None),
            NS(list_connection_workspaces=listing),'key',choice=lambda *a:pytest.fail('unexpected choice'),text=None)


async def test_repeated_cursor_is_rejected():
    async def listing(*a,**kw):return dict(items=[],has_more=True,next_workspace_id='same')
    with pytest.raises(ConnectorError,match='catalog'):
        await select_workspace(NS(workspace_id=None,workspace_label=None),NS(line=lambda _:None),
            NS(list_connection_workspaces=listing),'key',choice=None,text=None)
