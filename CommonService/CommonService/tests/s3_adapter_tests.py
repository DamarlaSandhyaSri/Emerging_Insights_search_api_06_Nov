# import asyncio
# import json
# import logging
# from datetime import datetime
# from CommonService.aws_resources.s3.s3_adapter import S3Adapter

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)


# REGION = "us-east-1"
# # PROFILE = "Comm-Prop-Sandbox"
# TEST_BUCKET = "ei-proquest-bucket"
# TEST_PREFIX = "test/"
# NON_EXISTENT_BUCKET = "non-existent-bucket-xyz-123"
# NON_EXISTENT_KEY = "non-existent-file-xyz-123.txt"


# class TestResult:
#     """Track test results"""
#     def __init__(self):
#         self.passed = 0
#         self.failed = 0
#         self.errors = []
    
#     def record_pass(self, test_name: str):
#         self.passed += 1
#         print(f"✅ PASSED: {test_name}")
    
#     def record_fail(self, test_name: str, error: str):
#         self.failed += 1
#         self.errors.append((test_name, error))
#         print(f"❌ FAILED: {test_name} - {error}")
    
#     def summary(self):
#         total = self.passed + self.failed
#         print("\n" + "=" * 70)
#         print(f"TEST SUMMARY: {self.passed}/{total} passed, {self.failed}/{total} failed")
#         if self.errors:
#             print("\nFailed Tests:")
#             for test_name, error in self.errors:
#                 print(f"  - {test_name}: {error}")
#         print("=" * 70)


# result = TestResult()


# # POSITIVE TEST CASES - Normal Operations
# async def test_01_valid_initialization():
#     """Test 1: Valid adapter initialization"""
#     test_name = "Valid Initialization"
#     try:
#         # s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#         s3 = S3Adapter(region=REGION)
#         assert s3.region == REGION
#         # assert s3.profile == PROFILE
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))


# async def test_02_explicit_connect():
#     """Test 2: Explicit connection"""
#     test_name = "Explicit Connect"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.connect()
#         assert s3._is_connected == True
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_03_auto_connect():
#     """Test 3: Auto-connect on first operation"""
#     test_name = "Auto-Connect"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         # Should auto-connect
#         objects = await s3.list_objects(TEST_BUCKET, max_keys=1)
#         assert s3._is_connected == True
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_04_list_objects_default():
#     """Test 4: List objects with default parameters"""
#     test_name = "List Objects - Default"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         objects = await s3.list_objects(TEST_BUCKET, max_keys=5)
#         assert isinstance(objects, list)
#         assert len(objects) <= 5
#         if objects:
#             assert "Key" in objects[0]
#             assert "Size" in objects[0]
#             assert "LastModified" in objects[0]
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_05_list_objects_sorted():
#     """Test 5: List objects sorted by date (newest first)"""
#     test_name = "List Objects - Sorted Newest First"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         objects = await s3.list_objects(TEST_BUCKET, max_keys=5, sort_by_modified=True, reverse=True)
#         if len(objects) > 1:
#             # Check if sorted in descending order
#             for i in range(len(objects) - 1):
#                 assert objects[i]["LastModified"] >= objects[i+1]["LastModified"]
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_06_list_objects_oldest_first():
#     """Test 6: List objects sorted (oldest first)"""
#     test_name = "List Objects - Sorted Oldest First"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         objects = await s3.list_objects(TEST_BUCKET, max_keys=5, reverse=False)
#         if len(objects) > 1:
#             # Check if sorted in ascending order
#             for i in range(len(objects) - 1):
#                 assert objects[i]["LastModified"] <= objects[i+1]["LastModified"]
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_07_list_objects_with_prefix():
#     """Test 7: List objects with prefix filter"""
#     test_name = "List Objects - With Prefix"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         objects = await s3.list_objects(TEST_BUCKET, prefix=TEST_PREFIX, max_keys=3)
#         # All keys should start with prefix
#         for obj in objects:
#             assert obj["Key"].startswith(TEST_PREFIX)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_08_write_read_text():
#     """Test 8: Write and read text object"""
#     test_name = "Write and Read Text"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-text-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
#     test_content = "Hello from S3 test!\nLine 2\nLine 3"
    
#     try:
#         # Write
#         await s3.write_object(TEST_BUCKET, test_key, test_content)
        
#         # Read
#         read_content = await s3.read_object(TEST_BUCKET, test_key)
#         assert read_content == test_content
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_09_write_read_json():
#     """Test 9: Write and read JSON object"""
#     test_name = "Write and Read JSON"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-json-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
#     test_data = {"name": "test", "value": 123, "items": [1, 2, 3]}
    
#     try:
#         # Write
#         json_content = json.dumps(test_data, indent=2)
#         await s3.write_object(TEST_BUCKET, test_key, json_content)
        
#         # Read
#         read_content = await s3.read_object(TEST_BUCKET, test_key)
#         read_data = json.loads(read_content)
#         assert read_data == test_data
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_10_write_read_bytes():
#     """Test 10: Write and read binary data"""
#     test_name = "Write and Read Bytes"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-binary-{datetime.now().strftime('%Y%m%d-%H%M%S')}.bin"
#     test_bytes = b'\x00\x01\x02\x03\xFF\xFE\xFD'
    
#     try:
#         # Write
#         await s3.write_bytes(TEST_BUCKET, test_key, test_bytes)
        
#         # Read
#         read_bytes = await s3.read_bytes(TEST_BUCKET, test_key)
#         assert read_bytes == test_bytes
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_11_object_exists_true():
#     """Test 11: Check object exists (should be True)"""
#     test_name = "Object Exists - True"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-exists-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Create object
#         await s3.write_object(TEST_BUCKET, test_key, "test content")
        
#         # Check exists
#         exists = await s3.object_exists(TEST_BUCKET, test_key)
#         assert exists == True
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_12_object_exists_false():
#     """Test 12: Check object exists (should be False)"""
#     test_name = "Object Exists - False"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
    
#     try:
#         exists = await s3.object_exists(TEST_BUCKET, NON_EXISTENT_KEY)
#         assert exists == False
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_13_delete_object():
#     """Test 13: Delete object"""
#     test_name = "Delete Object"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-delete-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Create object
#         await s3.write_object(TEST_BUCKET, test_key, "to be deleted")
        
#         # Verify exists
#         exists = await s3.object_exists(TEST_BUCKET, test_key)
#         assert exists == True
        
#         # Delete
#         await s3.delete_object(TEST_BUCKET, test_key)
        
#         # Verify deleted
#         exists = await s3.object_exists(TEST_BUCKET, test_key)
#         assert exists == False
        
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_14_update_object():
#     """Test 14: Update (overwrite) existing object"""
#     test_name = "Update Object"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-update-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Create original
#         await s3.write_object(TEST_BUCKET, test_key, "original content")
#         content1 = await s3.read_object(TEST_BUCKET, test_key)
        
#         # Update
#         await s3.write_object(TEST_BUCKET, test_key, "updated content")
#         content2 = await s3.read_object(TEST_BUCKET, test_key)
        
#         assert content1 == "original content"
#         assert content2 == "updated content"
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# # NEGATIVE TEST CASES - Error Handling
# async def test_15_invalid_region():
#     """Test 15: Invalid region should raise ValueError"""
#     test_name = "Invalid Region"
#     try:
#         s3 = S3Adapter(region="", profile_name=PROFILE)
#         result.record_fail(test_name, "Should have raised ValueError")
#     except ValueError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")


# async def test_16_missing_bucket_name():
#     """Test 16: Empty bucket name should raise ValueError"""
#     test_name = "Missing Bucket Name"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.list_objects("", max_keys=5)
#         result.record_fail(test_name, "Should have raised ValueError")
#     except ValueError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_17_missing_object_key():
#     """Test 17: Empty object key should raise ValueError"""
#     test_name = "Missing Object Key"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.read_object(TEST_BUCKET, "")
#         result.record_fail(test_name, "Should have raised ValueError")
#     except ValueError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_18_invalid_max_keys():
#     """Test 18: Invalid max_keys should raise ValueError"""
#     test_name = "Invalid Max Keys"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.list_objects(TEST_BUCKET, max_keys=0)
#         result.record_fail(test_name, "Should have raised ValueError")
#     except ValueError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_19_non_existent_bucket():
#     """Test 19: Non-existent bucket should raise FileNotFoundError"""
#     test_name = "Non-Existent Bucket"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.list_objects(NON_EXISTENT_BUCKET, max_keys=5)
#         result.record_fail(test_name, "Should have raised FileNotFoundError")
#     except FileNotFoundError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_20_read_non_existent_object():
#     """Test 20: Reading non-existent object should raise FileNotFoundError"""
#     test_name = "Read Non-Existent Object"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     try:
#         await s3.read_object(TEST_BUCKET, NON_EXISTENT_KEY)
#         result.record_fail(test_name, "Should have raised FileNotFoundError")
#     except FileNotFoundError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_21_empty_content_write():
#     """Test 21: Writing empty content should raise ValueError"""
#     test_name = "Empty Content Write"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-empty.txt"
#     try:
#         await s3.write_object(TEST_BUCKET, test_key, "")
#         result.record_fail(test_name, "Should have raised ValueError")
#     except ValueError:
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         await s3.disconnect()


# async def test_22_invalid_encoding():
#     """Test 22: Invalid encoding should raise ValueError"""
#     test_name = "Invalid Encoding"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-encoding-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Write with utf-8
#         await s3.write_object(TEST_BUCKET, test_key, "test content")
        
#         # Try to read with invalid encoding
#         await s3.read_object(TEST_BUCKET, test_key, encoding="invalid-encoding")
#         result.record_fail(test_name, "Should have raised error")
#     except (ValueError, LookupError):
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, f"Wrong exception: {type(e).__name__}")
#     finally:
#         try:
#             await s3.delete_object(TEST_BUCKET, test_key)
#         except:
#             pass
#         await s3.disconnect()


# async def test_23_multiple_operations_sequence():
#     """Test 23: Multiple operations in sequence"""
#     test_name = "Multiple Operations Sequence"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-multi-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Write
#         await s3.write_object(TEST_BUCKET, test_key, "step 1")
        
#         # Check exists
#         assert await s3.object_exists(TEST_BUCKET, test_key) == True
        
#         # Read
#         content = await s3.read_object(TEST_BUCKET, test_key)
#         assert content == "step 1"
        
#         # Update
#         await s3.write_object(TEST_BUCKET, test_key, "step 2")
#         content = await s3.read_object(TEST_BUCKET, test_key)
#         assert content == "step 2"
        
#         # Delete
#         await s3.delete_object(TEST_BUCKET, test_key)
#         assert await s3.object_exists(TEST_BUCKET, test_key) == False
        
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_24_concurrent_reads():
#     """Test 24: Concurrent read operations"""
#     test_name = "Concurrent Reads"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
#     test_key = f"{TEST_PREFIX}test-concurrent-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    
#     try:
#         # Create test object
#         await s3.write_object(TEST_BUCKET, test_key, "concurrent test")
        
#         # Perform concurrent reads
#         tasks = [s3.read_object(TEST_BUCKET, test_key) for _ in range(5)]
#         results = await asyncio.gather(*tasks)
        
#         # All should succeed with same content
#         assert all(r == "concurrent test" for r in results)
        
#         # Cleanup
#         await s3.delete_object(TEST_BUCKET, test_key)
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# async def test_25_disconnect_and_reconnect():
#     """Test 25: Disconnect and reconnect"""
#     test_name = "Disconnect and Reconnect"
#     s3 = S3Adapter(region=REGION, profile_name=PROFILE)
    
#     try:
#         # Connect
#         await s3.connect()
#         assert s3._is_connected == True
        
#         # Disconnect
#         await s3.disconnect()
#         assert s3._is_connected == False
        
#         # Reconnect on next operation
#         objects = await s3.list_objects(TEST_BUCKET, max_keys=1)
#         assert s3._is_connected == True
        
#         result.record_pass(test_name)
#     except Exception as e:
#         result.record_fail(test_name, str(e))
#     finally:
#         await s3.disconnect()


# # TEST RUNNER
# async def run_all_tests():
#     """Run all test cases"""
#     print("=" * 70)
#     print("S3Adapter Comprehensive Test Suite")
#     print("=" * 70)
#     print(f"Bucket: {TEST_BUCKET}")
#     print(f"Region: {REGION}")
#     print(f"Profile: {PROFILE}")
#     print("=" * 70)
    
#     tests = [
#         # Positive tests
#         test_01_valid_initialization,
#         test_02_explicit_connect,
#         test_03_auto_connect,
#         test_04_list_objects_default,
#         test_05_list_objects_sorted,
#         test_06_list_objects_oldest_first,
#         test_07_list_objects_with_prefix,
#         test_08_write_read_text,
#         test_09_write_read_json,
#         test_10_write_read_bytes,
#         test_11_object_exists_true,
#         test_12_object_exists_false,
#         test_13_delete_object,
#         test_14_update_object,
        
#         # Negative tests
#         test_15_invalid_region,
#         test_16_missing_bucket_name,
#         test_17_missing_object_key,
#         test_18_invalid_max_keys,
#         test_19_non_existent_bucket,
#         test_20_read_non_existent_object,
#         test_21_empty_content_write,
#         test_22_invalid_encoding,
        
#         # Additional tests
#         test_23_multiple_operations_sequence,
#         test_24_concurrent_reads,
#         test_25_disconnect_and_reconnect,
#     ]
    
#     print("\nRunning tests...\n")
    
#     for i, test in enumerate(tests, 1):
#         print(f"\n[{i}/{len(tests)}] Running: {test.__doc__}")
#         try:
#             await test()
#         except Exception as e:
#             result.record_fail(test.__name__, f"Test crashed: {str(e)}")
#         await asyncio.sleep(0.3)  # Small delay between tests
    
#     result.summary()


# async def run_single_test(test_num: int):
#     """Run a single test by number"""
#     tests = {
#         1: test_01_valid_initialization,
#         2: test_02_explicit_connect,
#         3: test_03_auto_connect,
#         4: test_04_list_objects_default,
#         5: test_05_list_objects_sorted,
#         6: test_06_list_objects_oldest_first,
#         7: test_07_list_objects_with_prefix,
#         8: test_08_write_read_text,
#         9: test_09_write_read_json,
#         10: test_10_write_read_bytes,
#         11: test_11_object_exists_true,
#         12: test_12_object_exists_false,
#         13: test_13_delete_object,
#         14: test_14_update_object,
#         15: test_15_invalid_region,
#         16: test_16_missing_bucket_name,
#         17: test_17_missing_object_key,
#         18: test_18_invalid_max_keys,
#         19: test_19_non_existent_bucket,
#         20: test_20_read_non_existent_object,
#         21: test_21_empty_content_write,
#         22: test_22_invalid_encoding,
#         23: test_23_multiple_operations_sequence,
#         24: test_24_concurrent_reads,
#         25: test_25_disconnect_and_reconnect,
#     }
    
#     if test_num in tests:
#         await tests[test_num]()
#         result.summary()
#     else:
#         print(f"Test {test_num} not found. Available: 1-{len(tests)}")


# if __name__ == "__main__":
#     # Run all tests
#     asyncio.run(run_all_tests())
    
#     # Or run a single test:
#     # asyncio.run(run_single_test(8))