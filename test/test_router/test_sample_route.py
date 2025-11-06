import unittest
from unittest.mock import AsyncMock

from httpx import ASGITransport, AsyncClient
from main_ import app

from src.core.config_loader import get_opensearch_client


class TestOpensearchMappings(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # self.client = TestClient(app)
        self.mock_opensearch = AsyncMock()
        self.mock_opensearch.indices = AsyncMock()
        self.mock_opensearch.indices.get_mappings = AsyncMock(return_value={"test_index": {"mappings": {}}})

        async def override_get_client():
            yield self.mock_opensearch

        app.dependency_overrides[get_opensearch_client] = override_get_client
        self.async_client = AsyncClient(transport=ASGITransport(app=app), base_url="http://ts")

    async def asyncTearDown(self):
        app.dependency_overrides.clear()
        await self.async_client.aclose()

    async def test_mappings_sucess(self):
        response = await self.async_client.get("/v1/mappings")
        self.assertEqual(response.status_code, 200)
        self.assertIn("test_index", response.json())

    # @patch("src.core.config_loader.get_opensearch_client", new_callable=AsyncMock)
    # async def test_mappings_error(self, mock_opensearch):
    #     print(mock_opensearch)
    #     print(mock_opensearch.indices.get_mappings)
    #     mock_opensearch.indices.get_mappings = AsyncMock(
    #         side_effect=Exception("Error connection")
    #     )
    #     mock_opensearch.indices.get_mappings.assert_called_once()
    #     res = self.client.get("/v1/mappings")
    #     self.assertEqual(res.status_code, 200)
    #     # self.assertIn("error", res.json())
