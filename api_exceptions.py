class API_BatchSizeTooLargeError(Exception):
    def __init__(self, batch_size: int, max_batch_size: int):
        super().__init__(f"Batch size: {batch_size} cannot be larger than the Spotify API limit: {max_batch_size}.")


class API_RateLimitError(Exception):
    def __init__(self, max_attempts: int):
        super().__init__(f"Max attempts at polling spotify API reached. "
                         f"Due to strict rate limits, please take a break an attempt later.")