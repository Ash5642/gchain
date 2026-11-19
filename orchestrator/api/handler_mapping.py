from anchors.handlers import *
from api.handlers import *
from orgs.handlers import *
from user.handlers import *
from docs.handlers import *
from bchain.handlers import *
handler_map = {
    "ANCHOR:CREATE":[
        AnchorCreate
    ],
    "ENTITY:CREATE":[
        EntityCreate
    ],
    "USER:CREATE":[
        UserCreate
    ],
    "SIGNATURES:MANAGE":[
        SignatureManagement
    ],
    "ORG:MANAGE":[
        OrganizationHandler
    ],
    "ORG:EMPLOYEE":[
        EmploymentManagement
    ],
    "ORG:ROLE":[
        RoleManagement
    ],
    "ORG:ROLE:EMPLOYEES":[
        RoleEmployees
    ],
    "GID":[
        GIdFetch
    ],
    "GID:assemble":[
        GIDAssemble
    ],
    "DOC:FAMILY:ADD":[
        DocumentFamilyCreate
    ],
    "DOC:GRANT:ADD":[
        DocumentGrantCreate
    ],
    "DOC:GRANT":[
        DocumentFamilyView
    ],
    "CHAIN":[
        ChainOptions
    ],
    "CHAIN:PUSH":[
        ChainPush
    ],
    "CHAIN:RESET":[
        ChainReset
    ],
    "HOST":[
        HostMessage
    ],
    "POLLUTE":[
        Pollute
    ],

    "DOCUMENT:ISSUE":[
        IssueDocument
    ],
    "FAMILY":[
        DocumentFamily
    ],
    "GRANT":[
        DocumentFamilyGrant
    ],
    "REVOKE":[
        Revoke
    ],
    "CASH":[
        Cash
    ],
    "EXECUTABLE":[
        Executable
    ],
    "DELETE":[
        Delete
    ],
    "CALL":[
        Call
    ]


}