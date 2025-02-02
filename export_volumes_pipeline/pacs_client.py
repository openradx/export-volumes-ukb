from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from queue import Empty, Queue
from typing import Iterable

import stamina
from pydicom import Dataset
from pynetdicom import evt
from pynetdicom._globals import STATUS_PENDING, STATUS_SUCCESS
from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.events import EventHandlerType
from pynetdicom.presentation import StoragePresentationContexts, build_role
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,  # type: ignore
    StudyRootQueryRetrieveInformationModelGet,  # type: ignore
)
from pynetdicom.status import code_to_category

from .errors import PacsError

logger = getLogger(__name__)


class PacsClient:
    def __init__(self, calling_ae_title: str, pacs_ae_title: str, pacs_host: str, pacs_port: int):
        self.calling_ae_title = calling_ae_title
        self.pacs_ae_title = pacs_ae_title
        self.pacs_host = pacs_host
        self.pacs_port = pacs_port

    @stamina.retry(on=PacsError, attempts=5, timeout=60)
    def find(self, query: dict) -> Iterable[Dataset]:
        ds = Dataset()
        for key, value in query.items():
            setattr(ds, key, value)

        ae = AE(ae_title=self.calling_ae_title)
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
        assoc = ae.associate(self.pacs_host, self.pacs_port, ae_title=self.pacs_ae_title)
        if assoc.is_established:
            responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
            for status, identifier in responses:
                status_category = code_to_category(status.Status)
                if status_category == STATUS_SUCCESS:
                    logger.debug("C-FIND completed successfully.")
                    break
                elif status_category == STATUS_PENDING:
                    if not identifier:
                        raise Exception("Missing identifier for pending C-FIND.")
                    yield identifier
                else:
                    raise PacsError(
                        "Connection timed out, was aborted or received invalid response"
                    )

            assoc.release()
        else:
            raise PacsError("Association rejected, aborted or never connected")

    @stamina.retry(on=PacsError, attempts=5, timeout=60)
    def retrieve(self, query: dict) -> Iterable[Dataset]:
        ds = Dataset()
        for key, value in query.items():
            setattr(ds, key, value)

        queue = Queue[Dataset]()

        def handle_store(event):
            ds = event.dataset
            ds.file_meta = event.file_meta
            queue.put(ds)
            return 0x0000

        ae = AE(ae_title=self.calling_ae_title)
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelGet)
        ext_neg = []
        for cx in StoragePresentationContexts[: 128 - 1]:
            assert cx.abstract_syntax is not None
            ae.add_requested_context(cx.abstract_syntax)
            ext_neg.append(build_role(cx.abstract_syntax, scp_role=True))

        def association_handler():
            handlers: list[EventHandlerType] = [(evt.EVT_C_STORE, handle_store)]
            assoc = ae.associate(
                self.pacs_host,
                self.pacs_port,
                ae_title=self.pacs_ae_title,
                ext_neg=ext_neg,
                evt_handlers=handlers,
            )

            if assoc.is_established:
                # Use the C-GET service to send the identifier
                responses = assoc.send_c_get(ds, StudyRootQueryRetrieveInformationModelGet)
                for status, identifier in responses:
                    if status:
                        logger.debug("C-GET query status: 0x{0:04x}".format(status.Status))
                    else:
                        raise PacsError(
                            "Connection timed out, was aborted or received invalid response"
                        )

                # Release the association
                assoc.release()
            else:
                raise PacsError("Association rejected, aborted or never connected")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(association_handler)

            while not future.done():
                try:
                    yield queue.get(timeout=1)
                except Empty:
                    continue

            future.result()
