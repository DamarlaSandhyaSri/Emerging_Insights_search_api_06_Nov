from CommonService.aws_resources.bedrock import TitanV1, TitanV2

# write unittest for TitanV1 and TitanV2
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import json
import numpy as np
class TestTitanEmbeddings(unittest.IsolatedAsyncioTestCase):
    
    TEST_INPUT_TEXT = "Hello world!"
    
    def setUp(self):
        # Mock session for testing
        self.mock_session = AsyncMock()
        
        # Mock response for TitanV1
        self.mock_v1_response = {
            "embedding": [0.1, 0.2, 0.3, 0.4, 0.5] * 300
        }
        
        # Mock response for TitanV2
        self.mock_v2_response = {
            "embedding": [0.2, 0.4, 0.6, 0.8, 1.0] * 204
        }

    async def test_titan_v1_embedding_without_normalize(self):
        """Test TitanV1 embedding generation without normalization"""
        # Create TitanV1 instance with mocked session
        titan_embedding = TitanV1(self.mock_session)
        
        # Mock the invoke_with_retry method directly
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Test the embedding generation
        payload = {"inputText": self.TEST_INPUT_TEXT, "normalize": False}
        response = await titan_embedding.generate_embedding(payload)
        
        # Assertions
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1500)  # Should return raw embedding
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v1", payload)

    async def test_titan_v1_embedding_with_normalize(self):
        """Test TitanV1 embedding generation with normalization"""
        # Create TitanV1 instance with mocked session
        titan_embedding = TitanV1(self.mock_session)
        
        # Mock the invoke_with_retry method directly
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Test the embedding generation with normalization
        payload = {"inputText": self.TEST_INPUT_TEXT, "normalize": True}
        response = await titan_embedding.generate_embedding(payload)
        
        # Assertions
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1500)
        
        # Verify normalization - the vector should be normalized (length = 1)
        vector_norm = np.linalg.norm(response)
        self.assertAlmostEqual(vector_norm, 1.0, places=5)
        
        # Verify that normalize was removed from payload before calling invoke_with_retry
        expected_payload = {"inputText": self.TEST_INPUT_TEXT}
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v1", expected_payload)

    async def test_titan_v2_embedding(self):
        """Test TitanV2 embedding generation"""
        # Create TitanV2 instance with mocked session
        titan_embedding = TitanV2(self.mock_session)
        
        # Mock the invoke_with_retry method directly
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v2_response["embedding"])
        
        # Test the embedding generation
        payload = {"inputText": self.TEST_INPUT_TEXT, "dimensions": 1024}
        response = await titan_embedding.generate_embedding(payload)
        
        # Assertions
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1020)  # Should match mock response
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v2:0", payload)

    def test_titan_v1_normalization_function(self):
        """Test the normalization function directly"""
        titan_embedding = TitanV1(self.mock_session)
        
        # Test with a simple vector
        test_vector = np.array([3.0, 4.0])
        normalized = titan_embedding._normalise_vector(test_vector)
        
        # Should be [0.6, 0.8] with length = 1
        expected = [0.6, 0.8]
        self.assertAlmostEqual(normalized[0], expected[0], places=5)
        self.assertAlmostEqual(normalized[1], expected[1], places=5)
        
        # Test with zero vector
        zero_vector = np.array([0.0, 0.0])
        result = titan_embedding._normalise_vector(zero_vector)
        self.assertEqual(result, [0.0, 0.0])

    async def test_error_handling(self):
        """Test error handling in embedding generation"""
        # Create TitanV1 instance with mocked session
        titan_embedding = TitanV1(self.mock_session)
        
        # Setup mock to raise an exception
        titan_embedding.invoke_with_retry = AsyncMock(side_effect=Exception("API Error"))
        
        # Should raise an exception
        with self.assertRaises(Exception):
            await titan_embedding.generate_embedding({"inputText": "test"})

    async def test_titan_v2_error_handling(self):
        """Test error handling in TitanV2 embedding generation"""
        # Create TitanV2 instance with mocked session
        titan_embedding = TitanV2(self.mock_session)
        
        # Setup mock to raise an exception
        titan_embedding.invoke_with_retry = AsyncMock(side_effect=RuntimeError("Model invocation failed"))
        
        # Should raise an exception
        with self.assertRaises(RuntimeError):
            await titan_embedding.generate_embedding({"inputText": "test"})

    async def test_titan_v1_with_none_payload(self):
        """Test TitanV1 with None payload"""
        titan_embedding = TitanV1(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Test with None payload
        response = await titan_embedding.generate_embedding(None)
        
        # Assertions
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1500)
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v1", {})

    async def test_titan_v2_with_none_payload(self):
        """Test TitanV2 with None payload"""
        titan_embedding = TitanV2(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v2_response["embedding"])
        
        # Test with None payload
        response = await titan_embedding.generate_embedding(None)
        
        # Assertions
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1020)
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v2:0", {})

    def test_titan_v1_initialization_with_kwargs(self):
        """Test TitanV1 initialization with custom parameters"""
        titan_embedding = TitanV1(self.mock_session, max_retries=5, retry_delay=2)
        
        self.assertEqual(titan_embedding.max_retries, 5)
        self.assertEqual(titan_embedding.retry_delay, 2)
        self.assertEqual(titan_embedding.model_id, "amazon.titan-embed-text-v1")

    def test_titan_v2_initialization_with_kwargs(self):
        """Test TitanV2 initialization with custom parameters"""
        titan_embedding = TitanV2(self.mock_session, max_retries=10, retry_delay=3)
        
        self.assertEqual(titan_embedding.max_retries, 10)
        self.assertEqual(titan_embedding.retry_delay, 3)
        self.assertEqual(titan_embedding.model_id, "amazon.titan-embed-text-v2:0")

    def test_titan_v1_initialization_default_values(self):
        """Test TitanV1 initialization with default values"""
        titan_embedding = TitanV1(self.mock_session)
        
        self.assertEqual(titan_embedding.max_retries, 3)
        self.assertEqual(titan_embedding.retry_delay, 1)

    def test_titan_v2_initialization_default_values(self):
        """Test TitanV2 initialization with default values"""
        titan_embedding = TitanV2(self.mock_session)
        
        self.assertEqual(titan_embedding.max_retries, 3)
        self.assertEqual(titan_embedding.retry_delay, 1)

    @patch('asyncio.sleep')
    async def test_invoke_with_retry_success_after_failure(self, mock_sleep):
        """Test successful retry after initial failure"""
        titan_embedding = TitanV1(self.mock_session, max_retries=3, retry_delay=1)
        
        # Mock session to fail once then succeed
        mock_response = MagicMock()
        mock_response["body"].read = AsyncMock(return_value=json.dumps({"embedding": [1, 2, 3]}).encode())
        
        self.mock_session.invoke_model = AsyncMock(side_effect=[
            Exception("Temporary failure"),
            mock_response
        ])
        
        payload = {"inputText": "test"}
        result = await titan_embedding.invoke_with_retry("test-model", payload)
        
        self.assertEqual(result, [1, 2, 3])
        self.assertEqual(self.mock_session.invoke_model.call_count, 2)
        mock_sleep.assert_called_once()

    @patch('asyncio.sleep')
    async def test_invoke_with_retry_max_retries_exceeded(self, mock_sleep):
        """Test that RuntimeError is raised when max retries are exceeded"""
        titan_embedding = TitanV1(self.mock_session, max_retries=2, retry_delay=1)
        
        # Mock session to always fail
        self.mock_session.invoke_model = AsyncMock(side_effect=Exception("Persistent failure"))
        
        payload = {"inputText": "test"}
        
        with self.assertRaises(RuntimeError) as context:
            await titan_embedding.invoke_with_retry("test-model", payload)
        
        self.assertIn("Failed to invoke model test-model after 2 attempts", str(context.exception))
        self.assertEqual(self.mock_session.invoke_model.call_count, 2)
        # Should sleep once (after first failure, before second attempt)
        mock_sleep.assert_called_once()

    @patch('random.uniform')
    @patch('asyncio.sleep')
    async def test_invoke_with_retry_exponential_backoff_with_jitter(self, mock_sleep, mock_uniform):
        """Test exponential backoff with jitter in retry logic"""
        titan_embedding = TitanV1(self.mock_session, max_retries=3, retry_delay=1)
        mock_uniform.return_value = 0.5  # Fixed jitter value
        
        # Mock session to fail twice then succeed
        mock_response = MagicMock()
        mock_response["body"].read = AsyncMock(return_value=json.dumps({"embedding": [1, 2, 3]}).encode())
        
        self.mock_session.invoke_model = AsyncMock(side_effect=[
            Exception("Failure 1"),
            Exception("Failure 2"),
            mock_response
        ])
        
        payload = {"inputText": "test"}
        result = await titan_embedding.invoke_with_retry("test-model", payload)
        
        self.assertEqual(result, [1, 2, 3])
        self.assertEqual(self.mock_session.invoke_model.call_count, 3)
        
        # Verify exponential backoff: first sleep = 1 + 0.5 = 1.5, second sleep = 2 + 0.5 = 2.5
        expected_calls = [unittest.mock.call(1.5), unittest.mock.call(2.5)]
        mock_sleep.assert_has_calls(expected_calls)

    async def test_invoke_with_retry_json_decode_error(self):
        """Test handling of JSON decode errors in invoke_with_retry"""
        titan_embedding = TitanV1(self.mock_session, max_retries=1)
        
        # Mock response with invalid JSON
        mock_response = MagicMock()
        mock_response["body"].read = AsyncMock(return_value=b"invalid json")
        self.mock_session.invoke_model = AsyncMock(return_value=mock_response)
        
        payload = {"inputText": "test"}
        
        with self.assertRaises(RuntimeError):
            await titan_embedding.invoke_with_retry("test-model", payload)

    async def test_invoke_with_retry_missing_embedding_key(self):
        """Test handling when response doesn't contain embedding key"""
        titan_embedding = TitanV1(self.mock_session, max_retries=1)
        
        # Mock response without embedding key
        mock_response = MagicMock()
        mock_response["body"].read = AsyncMock(return_value=json.dumps({"error": "no embedding"}).encode())
        self.mock_session.invoke_model = AsyncMock(return_value=mock_response)
        
        payload = {"inputText": "test"}
        
        with self.assertRaises(RuntimeError):
            await titan_embedding.invoke_with_retry("test-model", payload)

    def test_normalize_vector_edge_cases(self):
        """Test normalization with edge cases"""
        titan_embedding = TitanV1(self.mock_session)
        
        # Test with very small numbers
        small_vector = np.array([1e-10, 1e-10])
        result = titan_embedding._normalise_vector(small_vector)
        self.assertIsInstance(result, list)
        
        # Test with negative numbers
        negative_vector = np.array([-3.0, -4.0])
        result = titan_embedding._normalise_vector(negative_vector)
        expected_norm = np.linalg.norm(result)
        self.assertAlmostEqual(expected_norm, 1.0, places=5)
        
        # Test with single element
        single_vector = np.array([5.0])
        result = titan_embedding._normalise_vector(single_vector)
        self.assertEqual(result, [1.0])

    def test_model_id_constants(self):
        """Test that model IDs are correctly set"""
        titan_v1 = TitanV1(self.mock_session)
        titan_v2 = TitanV2(self.mock_session)
        
        self.assertEqual(titan_v1.model_id, "amazon.titan-embed-text-v1")
        self.assertEqual(titan_v2.model_id, "amazon.titan-embed-text-v2:0")
        self.assertEqual(TitanV1.model_id, "amazon.titan-embed-text-v1")
        self.assertEqual(TitanV2.model_id, "amazon.titan-embed-text-v2:0")

    async def test_titan_v1_generate_embedding_no_normalize_key(self):
        """Test TitanV1 generate_embedding when normalize key is not in payload"""
        titan_embedding = TitanV1(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Test without normalize key in payload
        payload = {"inputText": self.TEST_INPUT_TEXT}
        response = await titan_embedding.generate_embedding(payload)
        
        # Should return raw embedding (not normalized)
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1500)
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v1", payload)

    async def test_titan_v1_empty_payload(self):
        """Test TitanV1 with empty payload"""
        titan_embedding = TitanV1(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Test with empty payload
        response = await titan_embedding.generate_embedding({})
        
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1500)
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v1", {})

    async def test_titan_v2_empty_payload(self):
        """Test TitanV2 with empty payload"""
        titan_embedding = TitanV2(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v2_response["embedding"])
        
        # Test with empty payload
        response = await titan_embedding.generate_embedding({})
        
        self.assertIsInstance(response, list)
        self.assertEqual(len(response), 1020)
        titan_embedding.invoke_with_retry.assert_called_once_with("amazon.titan-embed-text-v2:0", {})

    @patch('CommonService.async_bedrock.base.logger')
    async def test_invoke_with_retry_logs_warnings(self, mock_logger):
        """Test that warnings are logged during retries"""
        titan_embedding = TitanV1(self.mock_session, max_retries=2, retry_delay=0.1)
        
        # Mock session to fail once then succeed
        mock_response = MagicMock()
        mock_response["body"].read = AsyncMock(return_value=json.dumps({"embedding": [1, 2, 3]}).encode())
        
        self.mock_session.invoke_model = AsyncMock(side_effect=[
            Exception("Temporary failure"),
            mock_response
        ])
        
        payload = {"inputText": "test"}
        result = await titan_embedding.invoke_with_retry("test-model", payload)
        
        self.assertEqual(result, [1, 2, 3])
        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        # Check that the warning message contains the expected text
        warning_call_args = mock_logger.warning.call_args[0][0]
        self.assertIn("Attempt 1 failed", warning_call_args)
        self.assertIn("Temporary failure", warning_call_args)

    def test_inheritance_structure(self):
        """Test that TitanV1 and TitanV2 properly inherit from base class"""
        titan_v1 = TitanV1(self.mock_session)
        titan_v2 = TitanV2(self.mock_session)
        
        # Test that both classes have the required attributes from base class
        self.assertTrue(hasattr(titan_v1, 'max_retries'))
        self.assertTrue(hasattr(titan_v1, 'retry_delay'))
        self.assertTrue(hasattr(titan_v1, 'invoke_with_retry'))
        self.assertTrue(hasattr(titan_v1, 'generate_embedding'))
        
        self.assertTrue(hasattr(titan_v2, 'max_retries'))
        self.assertTrue(hasattr(titan_v2, 'retry_delay'))
        self.assertTrue(hasattr(titan_v2, 'invoke_with_retry'))
        self.assertTrue(hasattr(titan_v2, 'generate_embedding'))

    async def test_concurrent_requests(self):
        """Test handling of concurrent embedding requests"""
        import asyncio
        
        titan_embedding = TitanV1(self.mock_session)
        titan_embedding.invoke_with_retry = AsyncMock(return_value=self.mock_v1_response["embedding"])
        
        # Create multiple concurrent requests
        tasks = []
        for i in range(3):
            payload = {"inputText": f"test {i}"}
            tasks.append(titan_embedding.generate_embedding(payload))
        
        # Run concurrently
        results = await asyncio.gather(*tasks)
        
        # Verify all requests completed successfully
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 1500)
        
        # Verify invoke_with_retry was called 3 times
        self.assertEqual(titan_embedding.invoke_with_retry.call_count, 3)


if __name__ == "__main__":
    unittest.main()