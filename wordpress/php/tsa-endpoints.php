add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summary',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summary($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_summary';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate', 'gameId', 'skaterFullName', 'playerId', 'homeRoad',
		'teamAbbrev', 'opponentTeamAbbrev', 'shootsCatches', 'positionCode',
		'gamesPlayed', 'goals', 'assists', 'points', 'plusMinus',
		'penaltyMinutes', 'pointsPerGame', 'evGoals', 'evPoints',
		'ppGoals', 'ppPoints', 'shGoals', 'shPoints', 'otGoals',
		'gameWinningGoals', 'shots', 'shootingPct', 'timeOnIcePerGame',
		'faceoffWinPct', 'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summary_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summary_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_summary');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summary-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_summary_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_summary_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_summary',
        'skater_summary'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-bios', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_bios',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_bios($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_bios';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $search = sanitize_text_field($request->get_param('search'));
    $teams_raw = sanitize_text_field($request->get_param('teams'));
    $positionCode = sanitize_text_field($request->get_param('positionCode'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if ($teams) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "currentTeamAbbrev IN ($placeholders)";
            foreach ($teams as $t) $params[] = $t;
        }
    }

    if (!empty($positionCode)) {
        $where[] = "positionCode = %s";
        $params[] = $positionCode;
    }

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
        $where[] = "skaterFullName LIKE %s";
        $params[] = $like;
    }

    $where_sql = $where ? "WHERE " . implode(" AND ", $where) : "";

    $total = $params
        ? intval($wpdb->get_var($wpdb->prepare("SELECT COUNT(*) FROM $table $where_sql", ...$params)))
        : intval($wpdb->get_var("SELECT COUNT(*) FROM $table"));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'skaterFullName',
		'playerId',
		'currentTeamAbbrev',
		'currentTeamName',
		'shootsCatches',
		'positionCode',
		'birthDate',
		'birthCity',
		'birthStateProvinceCode',
		'birthCountryCode',
		'nationalityCode',
		'height',
		'weight',
		'draftYear',
		'draftRound',
		'draftOverall',
		'firstSeasonForGameType',
		'isInHallOfFameYn',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'lastName'
	];
	
	$sort_field = 'skaterFullName';
	$sort_dir = 'ASC';

	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'desc') {
				$sort_dir = 'DESC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

	$sql = "SELECT *
			FROM $table
			$where_sql
			$order_sql
			LIMIT %d OFFSET %d";

    $rows = $wpdb->get_results(
        $wpdb->prepare($sql, ...array_merge($params, [$per_page, $offset])),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-bios-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_bios_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_bios_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_bios',
        'skater_bios',
        true
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffpercentages', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffpercentages',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffpercentages($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_faceoffpercentages';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame',
		'totalFaceoffs',
		'evFaceoffs',
		'ppFaceoffs',
		'shFaceoffs',
		'offensiveZoneFaceoffs',
		'neutralZoneFaceoffs',
		'defensiveZoneFaceoffs',
		'faceoffWinPct',
		'evFaceoffPct',
		'ppFaceoffPct',
		'shFaceoffPct',
		'offensiveZoneFaceoffPct',
		'neutralZoneFaceoffPct',
		'defensiveZoneFaceoffPct',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffpercentages-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffpercentages_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffpercentages_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_faceoffpercentages');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffpercentages-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_faceoffpercentages_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_faceoffpercentages_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_faceoffpercentages',
        'skater_faceoffpercentages'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffwins', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffwins',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffwins($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_faceoffwins';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'totalFaceoffs',
		'totalFaceoffWins',
		'totalFaceoffLosses',
		'faceoffWinPct',
		'evFaceoffs',
		'evFaceoffsWon',
		'evFaceoffsLost',
		'ppFaceoffs',
		'ppFaceoffsWon',
		'ppFaceoffsLost',
		'shFaceoffs',
		'shFaceoffsWon',
		'shFaceoffsLost',
		'offensiveZoneFaceoffs',
		'offensiveZoneFaceoffWins',
		'offensiveZoneFaceoffLosses',
		'neutralZoneFaceoffs',
		'neutralZoneFaceoffWins',
		'neutralZoneFaceoffLosses',
		'defensiveZoneFaceoffs',
		'defensiveZoneFaceoffWins',
		'defensiveZoneFaceoffLosses',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffwins-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_faceoffwins_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_faceoffwins_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_faceoffwins');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-faceoffwins-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_faceoffwins_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_faceoffwins_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_faceoffwins',
        'skater_faceoffwins'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-goalsforagainst', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_goalsforagainst',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_goalsforagainst($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_goalsforagainst';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'powerPlayTimeOnIcePerGame',
		'powerPlayGoalFor',
		'shortHandedGoalsAgainst',
		'shortHandedTimeOnIcePerGame',
		'shortHandedGoalsFor',
		'powerPlayGoalsAgainst',
		'evenStrengthTimeOnIcePerGame',
		'evenStrengthGoalsFor',
		'evenStrengthGoalsAgainst',
		'evenStrengthGoalDifference',
		'evenStrengthGoalsForPct',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-goalsforagainst-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_goalsforagainst_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_goalsforagainst_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_goalsforagainst');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-goalsforagainst-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_goalsforagainst_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_goalsforagainst_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_goalsforagainst',
        'skater_goalsforagainst'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penalties', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penalties',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penalties($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_penalties';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'points',
		'penaltyMinutes',
		'penaltySecondsPerGame',
		'timeOnIcePerGame',
		'penaltyMinutesPerTimeOnIce',
		'penaltiesDrawn',
		'penalties',
		'netPenalties',
		'penaltiesDrawnPer60',
		'penaltiesTakenPer60',
		'netPenaltiesPer60',
		'minorPenalties',
		'majorPenalties',
		'matchPenalties',
		'misconductPenalties',
		'gameMisconductPenalties',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penalties-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penalties_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penalties_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_penalties');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penalties-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penalties_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penalties_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_penalties',
        'skater_penalties'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltykill', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltykill',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltykill($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_penaltykill';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'shGoals',
		'shAssists',
		'shPrimaryAssists',
		'shSecondaryAssists',
		'shPoints',
		'shIndividualSatFor',
		'shShots',
		'shShootingPct',
		'shGoalsPer60',
		'shPrimaryAssistsPer60',
		'shSecondaryAssistsPer60',
		'shPointsPer60',
		'shIndividualSatForPer60',
		'shShotsPer60',
		'ppGoalsAgainstPer60',
		'shTimeOnIce',
		'shTimeOnIcePerGame',
		'shTimeOnIcePctPerGame',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltykill-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltykill_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltykill_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_penaltykill');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltykill-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penaltykill_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penaltykill_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_penaltykill',
        'skater_penaltykill'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltyshots', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltyshots',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltyshots($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_penaltyshots';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'penaltyShotAttempts',
		'penaltyShotsGoals',
		'penaltyShotsFailed',
		'penaltyShotShootingPct',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltyshots-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_penaltyshots_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_penaltyshots_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_penaltyshots');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-penaltyshots-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_penaltyshots_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_penaltyshots_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_penaltyshots',
        'skater_penaltyshots'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-percentages', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_percentages',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_percentages($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_percentages';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'satPercentage',
		'satPercentageAhead',
		'satPercentageTied',
		'satPercentageBehind',
		'satPercentageClose',
		'satRelative',
		'usatPercentage',
		'usatPercentageAhead',
		'usatPercentageTied',
		'usatPercentageBehind',
		'usatPrecentageClose',
		'usatRelative',
		'zoneStartPct5v5',
		'shootingPct5v5',
		'skaterSavePct5v5',
		'skaterShootingPlusSavePct5v5',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-percentages-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_percentages_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_percentages_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_percentages');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-percentages-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_percentages_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_percentages_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_percentages',
        'skater_percentages'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-powerplay', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_powerplay',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_powerplay($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_powerplay';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'ppGoals',
		'ppAssists',
		'ppPrimaryAssists',
		'ppSecondaryAssists',
		'ppPoints',
		'ppIndividualSatFor',
		'ppShots',
		'ppShootingPct',
		'ppGoalsPer60',
		'ppPrimaryAssistsPer60',
		'ppSecondaryAssistsPer60',
		'ppPointsPer60',
		'ppIndividualSatForPer60',
		'ppShotsPer60',
		'ppGoalsForPer60',
		'ppTimeOnIce',
		'ppTimeOnIcePerGame',
		'ppTimeOnIcePctPerGame',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-powerplay-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_powerplay_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_powerplay_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_powerplay');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-powerplay-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_powerplay_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_powerplay_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_powerplay',
        'skater_powerplay'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-puckpossessions', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_puckpossessions',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_puckpossessions($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_puckpossessions';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'satPct',
		'usatPct',
		'goalsPct',
		'individualSatForPer60',
		'individualShotsForPer60',
		'onIceShootingPct',
		'zoneStartPct',
		'offensiveZoneStartRatio',
		'offensiveZoneStartPct',
		'neutralZoneStartPct',
		'defensiveZoneStartPct',
		'faceoffPct5v5',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-puckpossessions-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_puckpossessions_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_puckpossessions_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_puckpossessions');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-puckpossessions-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_puckpossessions_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_puckpossessions_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_puckpossessions',
        'skater_puckpossessions'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-realtime', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_realtime',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_realtime($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_realtime';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame',
		'hits',
		'hitsPer60',
		'blockedShots',
		'blockedShotsPer60',
		'giveaways',
		'giveawaysPer60',
		'takeaways',
		'takeawaysPer60',
		'firstGoals',
		'otGoals',
		'emptyNetGoals',
		'emptyNetAssists',
		'emptyNetPoints',
		'totalShotAttempts',
		'shotAttemptsBlocked',
		'missedShots',
		'missedShotWideOfNet',
		'missedShotOverNet',
		'missedShotGoalpost',
		'missedShotCrossbar',
		'missedShotShort',
		'missedShotFailedBankAttempt',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-realtime-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_realtime_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_realtime_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_realtime');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-realtime-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_realtime_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_realtime_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_realtime',
        'skater_realtime'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringpergame', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringpergame',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringpergame($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_scoringpergame';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'goals',
		'assists',
		'totalPrimaryAssists',
		'totalSecondaryAssists',
		'points',
		'shots',
		'penaltyMinutes',
		'hits',
		'blockedShots',
		'timeOnIce',
		'goalsPerGame',
		'assistsPerGame',
		'primaryAssistsPerGame',
		'secondaryAssistsPerGame',
		'pointsPerGame',
		'shotsPerGame',
		'penaltyMinutesPerGame',
		'hitsPerGame',
		'blocksPerGame',
		'timeOnIcePerGame',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringpergame-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringpergame_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringpergame_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_scoringpergame');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringpergame-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_scoringpergame_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_scoringpergame_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_scoringpergame',
        'skater_scoringpergame'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringrates', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringrates',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringrates($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_scoringrates';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'goals5v5',
		'assists5v5',
		'primaryAssists5v5',
		'secondaryAssists5v5',
		'points5v5',
		'goalsPer605v5',
		'assistsPer605v5',
		'primaryAssistsPer605v5',
		'secondaryAssistsPer605v5',
		'pointsPer605v5',
		'shootingPct5v5',
		'onIceShootingPct5v5',
		'offensiveZoneStartPct5v5',
		'satRelative5v5',
		'satPct',
		'netMinorPenaltiesPer60',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringrates-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_scoringrates_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_scoringrates_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_scoringrates');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-scoringrates-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_scoringrates_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_scoringrates_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_scoringrates',
        'skater_scoringrates'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shootout', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_shootout',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_shootout($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_shootout';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'shootoutGamesPlayed',
		'shootoutGoals',
		'shootoutShots',
		'shootoutShootingPct',
		'shootoutGameDecidingGoals',
		'careerShootoutGamesPlayed',
		'careerShootoutGoals',
		'careerShootoutShots',
		'careerShootoutShootingPct',
		'careerShootoutGameDecidingGoals',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shootout-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_shootout_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_shootout_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_shootout');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shootout-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_shootout_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_shootout_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_shootout',
        'skater_shootout'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shottype', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_shottype',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_shottype($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_shottype';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	// Shot Type table does not include positionCode, so position filtering is skipped.

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'gamesPlayed',
		'goals',
		'goalsWrist',
		'goalsSnap',
		'goalsSlap',
		'goalsBackhand',
		'goalsTipIn',
		'goalsDeflected',
		'goalsWrapAround',
		'goalsPoke',
		'goalsCradle',
		'goalsBetweenLegs',
		'goalsBat',
		'shotsOnNetWrist',
		'shotsOnNetSnap',
		'shotsOnNetSlap',
		'shotsOnNetBackhand',
		'shotsOnNetTipIn',
		'shotsOnNetDeflected',
		'shotsOnNetWrapAround',
		'shotsOnNetPoke',
		'shotsOnNetCradle',
		'shotsOnNetBetweenLegs',
		'shotsOnNetBat',
		'shootingPct',
		'shootingPctWrist',
		'shootingPctSnap',
		'shootingPctSlap',
		'shootingPctBackhand',
		'shootingPctTipIn',
		'shootingPctDeflected',
		'shootingPctWrapAround',
		'shootingPctPoke',
		'shootingPctCradle',
		'shootingPctBetweenLegs',
		'shootingPctBat',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shottype-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_shottype_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_shottype_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_shottype');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-shottype-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_shottype_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_shottype_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_shottype',
        'skater_shottype',
        false,
        false
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summaryshooting', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summaryshooting',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summaryshooting($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_summaryshooting';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIcePerGame5v5',
		'satFor',
		'satAgainst',
		'satTotal',
		'satAhead',
		'satTied',
		'satBehind',
		'satClose',
		'satRelative',
		'usatFor',
		'usatAgainst',
		'usatTotal',
		'usatAhead',
		'usatTied',
		'usatBehind',
		'usatClose',
		'usatRelative',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summaryshooting-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_summaryshooting_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_summaryshooting_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_summaryshooting');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-summaryshooting-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_summaryshooting_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_summaryshooting_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_summaryshooting',
        'skater_summaryshooting'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-timeonice', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_timeonice',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_timeonice($request) {
    global $wpdb;
	
	tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_skater_timeonice';

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
	$opponents_raw = sanitize_text_field($request->get_param('opponents'));
	$homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$positionCode = sanitize_text_field($request->get_param('positionCode'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";
            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }
	
	if (!empty($opponents_raw)) {
		$opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
		if (!empty($opponents)) {
			$placeholders = implode(',', array_fill(0, count($opponents), '%s'));
			$where[] = "opponentTeamAbbrev IN ($placeholders)";
			foreach ($opponents as $opponent) {
				$params[] = $opponent;
			}
		}
	}
	
	if (!empty($homeRoad)) {
		$where[] = "homeRoad = %s";
		$params[] = $homeRoad;
	}
	
	if (!empty($positionCode)) {
		$where[] = "positionCode = %s";
		$params[] = $positionCode;
	}

    if (!empty($search)) {
        $like = '%' . $wpdb->esc_like($search) . '%';
		$where[] = "skaterFullName LIKE %s";
		$params[] = $like;
    }

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));
	
	$allowed_sort_fields = [
		'gameDate',
		'gameId',
		'skaterFullName',
		'playerId',
		'homeRoad',
		'teamAbbrev',
		'opponentTeamAbbrev',
		'shootsCatches',
		'positionCode',
		'gamesPlayed',
		'timeOnIce',
		'evTimeOnIce',
		'ppTimeOnIce',
		'shTimeOnIce',
		'timeOnIcePerGame',
		'evTimeOnIcePerGame',
		'ppTimeOnIcePerGame',
		'shTimeOnIcePerGame',
		'otTimeOnIce',
		'otTimeOnIcePerOtGame',
		'shifts',
		'timeOnIcePerShift',
		'shiftsPerGame',
		'lastName'
	];

	$sort_field = 'gameDate';
	$sort_dir = 'DESC';

	// Tabulator usually sends "sort", not "sorters"
	$sorters = $request->get_param('sort');

	if (empty($sorters)) {
		$sorters = $request->get_param('sorters');
	}

	// Sometimes it arrives as JSON string
	if (is_string($sorters)) {
		$decoded = json_decode($sorters, true);
		if (json_last_error() === JSON_ERROR_NONE) {
			$sorters = $decoded;
		}
	}

	if (!empty($sorters) && is_array($sorters)) {
		$first_sorter = $sorters[0] ?? null;

		if (is_array($first_sorter)) {
			if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
				$sort_field = $first_sorter['field'];
			}

			if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
				$sort_dir = 'ASC';
			}
		}
	}

	$order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-timeonice-meta', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_timeonice_meta',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_timeonice_meta($request) {
    return tsa_get_skater_date_meta_for_table('tsa_skater_timeonice');
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-timeonice-csv', [
        'methods' => 'GET',
        'callback' => 'tsa_download_skater_timeonice_csv',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_download_skater_timeonice_csv($request) {
    return tsa_stream_skater_csv_for_table(
        $request,
        'tsa_skater_timeonice',
        'skater_timeonice'
    );
}

add_action('rest_api_init', function () {
    register_rest_route('tsa/v1', '/skater-team-options', [
        'methods' => 'GET',
        'callback' => 'tsa_get_skater_team_options',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_skater_team_options($request) {
    global $wpdb;

    $bios_table = $wpdb->prefix . 'tsa_skater_bios';
    $summary_table = $wpdb->prefix . 'tsa_skater_summary';

    $teams = $wpdb->get_col("
        SELECT team FROM (
            SELECT DISTINCT currentTeamAbbrev AS team
            FROM $bios_table
            WHERE currentTeamAbbrev <> ''

            UNION

            SELECT DISTINCT teamAbbrev AS team
            FROM $summary_table
            WHERE teamAbbrev <> ''

            UNION

            SELECT DISTINCT opponentTeamAbbrev AS team
            FROM $summary_table
            WHERE opponentTeamAbbrev <> ''
        ) AS x
        ORDER BY team
    ");

    return [
        'teams' => $teams,
        'opponents' => $teams
    ];
}

function tsa_set_utf8mb4() {
    global $wpdb;

    $wpdb->query("SET NAMES utf8mb4");
    $wpdb->query("SET CHARACTER SET utf8mb4");
}

function tsa_get_skater_date_meta_for_table($table_name) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table = $wpdb->prefix . $table_name;

    return [
        'min_date' => $wpdb->get_var("SELECT MIN(gameDate) FROM $table"),
        'max_date' => $wpdb->get_var("SELECT MAX(gameDate) FROM $table"),
    ];
}

function tsa_stream_skater_csv_for_table($request, $table_name, $filename_base, $is_bios = false, $has_position = true) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table = $wpdb->prefix . $table_name;
    $full = intval($request->get_param('full')) === 1;

    $where = [];
    $params = [];

    if (!$full) {
        $teams_raw = sanitize_text_field($request->get_param('teams'));
        $opponents_raw = sanitize_text_field($request->get_param('opponents'));
        $homeRoad = sanitize_text_field($request->get_param('homeRoad'));
        $positionCode = sanitize_text_field($request->get_param('positionCode'));
        $search = sanitize_text_field($request->get_param('search'));
        $date_single = sanitize_text_field($request->get_param('date_single'));
        $date_start = sanitize_text_field($request->get_param('date_start'));
        $date_end = sanitize_text_field($request->get_param('date_end'));

        if (!empty($teams_raw)) {
            $teams = array_filter(array_map('trim', explode(',', $teams_raw)));
            if ($teams) {
                $placeholders = implode(',', array_fill(0, count($teams), '%s'));
                $team_col = $is_bios ? 'currentTeamAbbrev' : 'teamAbbrev';
                $where[] = "$team_col IN ($placeholders)";
                foreach ($teams as $team) {
                    $params[] = $team;
                }
            }
        }

        if (!$is_bios && !empty($opponents_raw)) {
            $opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));
            if ($opponents) {
                $placeholders = implode(',', array_fill(0, count($opponents), '%s'));
                $where[] = "opponentTeamAbbrev IN ($placeholders)";
                foreach ($opponents as $opponent) {
                    $params[] = $opponent;
                }
            }
        }

        if (!$is_bios && !empty($homeRoad)) {
            $where[] = "homeRoad = %s";
            $params[] = $homeRoad;
        }

		if ($has_position && !empty($positionCode)) {
			$where[] = "positionCode = %s";
			$params[] = $positionCode;
		}

        if (!empty($search)) {
            $like = '%' . $wpdb->esc_like($search) . '%';
            $where[] = "(skaterFullName LIKE %s)";
            $params[] = $like;
        }

        if (!$is_bios) {
            if (!empty($date_single)) {
                $where[] = "gameDate = %s";
                $params[] = $date_single;
            } elseif (!empty($date_start) && !empty($date_end)) {
                $where[] = "gameDate BETWEEN %s AND %s";
                $params[] = $date_start;
                $params[] = $date_end;
            }
        }
    }

    $where_sql = $where ? "WHERE " . implode(" AND ", $where) : "";
    $order_sql = $is_bios ? "ORDER BY skaterFullName ASC" : "ORDER BY gameDate DESC";

    $sql = "SELECT * FROM $table $where_sql $order_sql";

    $rows = $params
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);
		
	if (empty($rows)) {
		wp_die('No rows found for selected filters.');
	}

    header('Content-Type: text/csv; charset=utf-8');

    $filename = $full
        ? "full_{$filename_base}.csv"
        : "filtered_{$filename_base}.csv";

    header("Content-Disposition: attachment; filename={$filename}");
	
	echo "\xEF\xBB\xBF";

	$out = fopen('php://output', 'w');

	if (!empty($rows)) {
		fputcsv($out, array_keys($rows[0]), ',', '"', '\\');

		foreach ($rows as $row) {
			fputcsv($out, $row, ',', '"', '\\');
		}
	}

	fclose($out);
	exit;
}



function tsa_get_team_dataset_map() {
    return [
        'summary' => 'tsa_team_summary',
        'daysbetweengames' => 'tsa_team_daysbetweengames',
        'faceoffpercentages' => 'tsa_team_faceoffpercentages',
        'faceoffwins' => 'tsa_team_faceoffwins',
        'goalgames' => 'tsa_team_goalgames',
        'goalsagainstbystrength' => 'tsa_team_goalsagainstbystrength',
        'goalsbyperiod' => 'tsa_team_goalsbyperiod',
        'goalsforbystrength' => 'tsa_team_goalsforbystrength',
        'goalsforbystrengthgoaliepull' => 'tsa_team_goalsforbystrengthgoaliepull',
        'leadingtrailing' => 'tsa_team_leadingtrailing',
        'outshootoutshotby' => 'tsa_team_outshootoutshotby',
        'penalties' => 'tsa_team_penalties',
        'penaltykill' => 'tsa_team_penaltykill',
        'penaltykilltime' => 'tsa_team_penaltykilltime',
        'percentages' => 'tsa_team_percentages',
        'powerplay' => 'tsa_team_powerplay',
        'powerplaytime' => 'tsa_team_powerplaytime',
        'realtime' => 'tsa_team_realtime',
        'scoretrailfirst' => 'tsa_team_scoretrailfirst',
        'shootout' => 'tsa_team_shootout',
        'shottype' => 'tsa_team_shottype',
        'summaryshooting' => 'tsa_team_summaryshooting',
    ];
}

add_action('rest_api_init', function () {
    foreach (tsa_get_team_dataset_map() as $dataset => $table_name) {
        register_rest_route('tsa/v1', "/team-$dataset", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_team_dataset($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/team-$dataset-meta", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_team_date_meta($dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/team-$dataset-csv", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_stream_team_csv($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);
    }

    register_rest_route('tsa/v1', '/team-team-options', [
        'methods' => 'GET',
        'callback' => 'tsa_get_team_team_options',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_team_table_name($dataset) {
    $map = tsa_get_team_dataset_map();

    if (!isset($map[$dataset])) {
        return null;
    }

    return $map[$dataset];
}

function tsa_get_team_allowed_columns($table) {
    global $wpdb;

    $columns = $wpdb->get_results("SHOW COLUMNS FROM `$table`", ARRAY_A);

    return array_map(function ($col) {
        return $col['Field'];
    }, $columns);
}

function tsa_get_team_dataset($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_team_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid team dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
    $opponents_raw = sanitize_text_field($request->get_param('opponents'));
    $homeRoad = sanitize_text_field($request->get_param('homeRoad'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    $where = [];
    $params = [];

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));

        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";

            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }

    if (!empty($opponents_raw)) {
        $opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));

        if (!empty($opponents)) {
            $placeholders = implode(',', array_fill(0, count($opponents), '%s'));
            $where[] = "opponentTeamAbbrev IN ($placeholders)";

            foreach ($opponents as $opponent) {
                $params[] = $opponent;
            }
        }
    }

    if (!empty($homeRoad)) {
        $where[] = "homeRoad = %s";
        $params[] = $homeRoad;
    }

	if (!empty($search)) {
		$search = trim($search);
		$upper_search = strtoupper($search);

		if (strlen($search) <= 3) {
			$like = $wpdb->esc_like($upper_search) . '%';
			$where[] = "teamAbbrev LIKE %s";
			$params[] = $like;
		} else {
			$like = '%' . $wpdb->esc_like($search) . '%';
			$where[] = "(teamAbbrev LIKE %s OR teamFullName LIKE %s)";
			$params[] = $like;
			$params[] = $like;
		}
	}

    if (!empty($date_single)) {
        $where[] = "gameDate = %s";
        $params[] = $date_single;
    } elseif (!empty($date_start) && !empty($date_end)) {
        $where[] = "gameDate BETWEEN %s AND %s";
        $params[] = $date_start;
        $params[] = $date_end;
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM `$table` $where_sql";

    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));

    $allowed_sort_fields = tsa_get_team_allowed_columns($table);

    $sort_field = in_array('gameDate', $allowed_sort_fields, true) ? 'gameDate' : 'teamFullName';
    $sort_dir = 'DESC';

    $sorters = $request->get_param('sort');

    if (empty($sorters)) {
        $sorters = $request->get_param('sorters');
    }

    if (is_string($sorters)) {
        $decoded = json_decode($sorters, true);

        if (json_last_error() === JSON_ERROR_NONE) {
            $sorters = $decoded;
        }
    }

    if (!empty($sorters) && is_array($sorters)) {
        $first_sorter = $sorters[0] ?? null;

        if (is_array($first_sorter)) {
            if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
                $sort_field = $first_sorter['field'];
            }

            if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
                $sort_dir = 'ASC';
            }
        }
    }

    $order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM `$table`
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

function tsa_get_team_date_meta($dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_team_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid team dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;

    return [
        'min_date' => $wpdb->get_var("SELECT MIN(gameDate) FROM `$table`"),
        'max_date' => $wpdb->get_var("SELECT MAX(gameDate) FROM `$table`"),
    ];
}

function tsa_get_team_team_options($request) {
    global $wpdb;

    tsa_set_utf8mb4();

    $summary_table = $wpdb->prefix . 'tsa_team_summary';

    $teams = $wpdb->get_col("
        SELECT DISTINCT teamAbbrev
        FROM `$summary_table`
        WHERE teamAbbrev <> ''
        ORDER BY teamAbbrev
    ");

    $opponents = $wpdb->get_col("
        SELECT DISTINCT opponentTeamAbbrev
        FROM `$summary_table`
        WHERE opponentTeamAbbrev <> ''
        ORDER BY opponentTeamAbbrev
    ");

    return [
        'teams' => $teams,
        'opponents' => $opponents,
    ];
}

function tsa_stream_team_csv($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_team_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid team dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;
    $full = intval($request->get_param('full')) === 1;

    $where = [];
    $params = [];

    if (!$full) {
        $teams_raw = sanitize_text_field($request->get_param('teams'));
        $opponents_raw = sanitize_text_field($request->get_param('opponents'));
        $homeRoad = sanitize_text_field($request->get_param('homeRoad'));
        $search = sanitize_text_field($request->get_param('search'));
        $date_single = sanitize_text_field($request->get_param('date_single'));
        $date_start = sanitize_text_field($request->get_param('date_start'));
        $date_end = sanitize_text_field($request->get_param('date_end'));

        if (!empty($teams_raw)) {
            $teams = array_filter(array_map('trim', explode(',', $teams_raw)));

            if (!empty($teams)) {
                $placeholders = implode(',', array_fill(0, count($teams), '%s'));
                $where[] = "teamAbbrev IN ($placeholders)";

                foreach ($teams as $team) {
                    $params[] = $team;
                }
            }
        }

        if (!empty($opponents_raw)) {
            $opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));

            if (!empty($opponents)) {
                $placeholders = implode(',', array_fill(0, count($opponents), '%s'));
                $where[] = "opponentTeamAbbrev IN ($placeholders)";

                foreach ($opponents as $opponent) {
                    $params[] = $opponent;
                }
            }
        }

        if (!empty($homeRoad)) {
            $where[] = "homeRoad = %s";
            $params[] = $homeRoad;
        }

		if (!empty($search)) {
			$search = trim($search);
			$upper_search = strtoupper($search);

			if (strlen($search) <= 3) {
				$like = $wpdb->esc_like($upper_search) . '%';
				$where[] = "teamAbbrev LIKE %s";
				$params[] = $like;
			} else {
				$like = '%' . $wpdb->esc_like($search) . '%';
				$where[] = "(teamAbbrev LIKE %s OR teamFullName LIKE %s)";
				$params[] = $like;
				$params[] = $like;
			}
		}

        if (!empty($date_single)) {
            $where[] = "gameDate = %s";
            $params[] = $date_single;
        } elseif (!empty($date_start) && !empty($date_end)) {
            $where[] = "gameDate BETWEEN %s AND %s";
            $params[] = $date_start;
            $params[] = $date_end;
        }
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";
    $sql = "SELECT * FROM `$table` $where_sql ORDER BY gameDate DESC";

    $rows = !empty($params)
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);

    if (empty($rows)) {
        wp_die('No rows found for selected filters.');
    }

    header('Content-Type: text/csv; charset=utf-8');

    $filename = $full
        ? "full_team_{$dataset}.csv"
        : "filtered_team_{$dataset}.csv";

    header("Content-Disposition: attachment; filename={$filename}");

    echo "\xEF\xBB\xBF";

    $out = fopen('php://output', 'w');

    fputcsv($out, array_keys($rows[0]), ',', '"', '\\');

    foreach ($rows as $row) {
        fputcsv($out, $row, ',', '"', '\\');
    }

    fclose($out);
    exit;
}




function tsa_get_shot_events_dataset_map() {
    return [
        'playerlocationpriors' => 'tsa_shot_events_player_location_priors',
        'shotlocationevents' => 'tsa_shot_events_shot_location_events',
    ];
}

add_action('rest_api_init', function () {
    foreach (tsa_get_shot_events_dataset_map() as $dataset => $table_name) {
        register_rest_route('tsa/v1', "/shot-events-$dataset", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_shot_events_dataset($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/shot-events-$dataset-meta", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_shot_events_date_meta($dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/shot-events-$dataset-csv", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_stream_shot_events_csv($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);
    }

    register_rest_route('tsa/v1', '/shot-events-options', [
        'methods' => 'GET',
        'callback' => 'tsa_get_shot_events_options',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_shot_events_table_name($dataset) {
    $map = tsa_get_shot_events_dataset_map();

    if (!isset($map[$dataset])) {
        return null;
    }

    return $map[$dataset];
}

function tsa_get_shot_events_allowed_columns($table) {
    global $wpdb;

    $columns = $wpdb->get_results("SHOW COLUMNS FROM `$table`", ARRAY_A);

    return array_map(function ($col) {
        return $col['Field'];
    }, $columns);
}

function tsa_shot_events_table_has_column($allowed_columns, $column) {
    return in_array($column, $allowed_columns, true);
}

function tsa_apply_shot_events_filters($request, $allowed_columns, &$where, &$params) {
    global $wpdb;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
    $opponents_raw = sanitize_text_field($request->get_param('opponents'));
    $homeRoad = sanitize_text_field($request->get_param('homeRoad'));
	$shot_region = sanitize_text_field($request->get_param('shot_region'));
    $shotType = sanitize_text_field($request->get_param('shotType'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    if (tsa_shot_events_table_has_column($allowed_columns, 'teamAbbrev') && !empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));

        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));
            $where[] = "teamAbbrev IN ($placeholders)";

            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }

    if (tsa_shot_events_table_has_column($allowed_columns, 'awayAbbrev') && !empty($opponents_raw)) {
        $opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));

        if (!empty($opponents)) {
            $placeholders = implode(',', array_fill(0, count($opponents), '%s'));
            $where[] = "(
                CASE
                    WHEN homeRoad = 'H' THEN awayAbbrev
                    WHEN homeRoad = 'R' THEN homeAbbrev
                    ELSE ''
                END
            ) IN ($placeholders)";

            foreach ($opponents as $opponent) {
                $params[] = $opponent;
            }
        }
    }

    if (tsa_shot_events_table_has_column($allowed_columns, 'homeRoad') && !empty($homeRoad)) {
        $where[] = "homeRoad = %s";
        $params[] = $homeRoad;
    }
	
	if (tsa_shot_events_table_has_column($allowed_columns, 'shot_region') && $shot_region !== '') {
		$where[] = "`shot_region` = %s";
		$params[] = $shot_region;
	}

    if (tsa_shot_events_table_has_column($allowed_columns, 'shotType') && !empty($shotType)) {
        $where[] = "shotType = %s";
        $params[] = $shotType;
    }

    if (!empty($search)) {
        $search = trim($search);
        $like = '%' . $wpdb->esc_like($search) . '%';
        $search_parts = [];

        foreach (['shooter', 'teamAbbrev', 'homeAbbrev', 'awayAbbrev', 'shotType', 'shot_region'] as $col) {
            if (tsa_shot_events_table_has_column($allowed_columns, $col)) {
                $search_parts[] = "$col LIKE %s";
                $params[] = $like;
            }
        }

		if (ctype_digit($search)) {
			foreach (['playerId', 'gameId', 'eventId', 'teamId', 'goalieInNetId'] as $col) {
				if (tsa_shot_events_table_has_column($allowed_columns, $col)) {
					$search_parts[] = "CAST($col AS CHAR) LIKE %s";
					$params[] = $like;
				}
			}
		}

        if (!empty($search_parts)) {
            $where[] = '(' . implode(' OR ', $search_parts) . ')';
        }
    }

    if (tsa_shot_events_table_has_column($allowed_columns, 'date')) {
        if (!empty($date_single)) {
            $where[] = "date = %s";
            $params[] = $date_single;
        } elseif (!empty($date_start) && !empty($date_end)) {
            $where[] = "date BETWEEN %s AND %s";
            $params[] = $date_start;
            $params[] = $date_end;
        }
    }
}

function tsa_get_shot_events_dataset($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_shot_events_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid shot_events dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $allowed_sort_fields = tsa_get_shot_events_allowed_columns($table);

    $where = [];
    $params = [];

    tsa_apply_shot_events_filters($request, $allowed_sort_fields, $where, $params);

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM `$table` $where_sql";

    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));

    if (in_array('date', $allowed_sort_fields, true)) {
        $sort_field = 'date';
    } elseif (in_array('total_shots', $allowed_sort_fields, true)) {
        $sort_field = 'total_shots';
    } else {
        $sort_field = 'playerId';
    }

    $sort_dir = 'DESC';

    $sorters = $request->get_param('sort');

    if (empty($sorters)) {
        $sorters = $request->get_param('sorters');
    }

    if (is_string($sorters)) {
        $decoded = json_decode($sorters, true);

        if (json_last_error() === JSON_ERROR_NONE) {
            $sorters = $decoded;
        }
    }

    if (!empty($sorters) && is_array($sorters)) {
        $first_sorter = $sorters[0] ?? null;

        if (is_array($first_sorter)) {
            if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_sort_fields, true)) {
                $sort_field = $first_sorter['field'];
            }

            if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
                $sort_dir = 'ASC';
            }
        }
    }

    $order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM `$table`
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

function tsa_get_shot_events_date_meta($dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_shot_events_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid shot_events dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;
    $allowed_columns = tsa_get_shot_events_allowed_columns($table);

    if (!tsa_shot_events_table_has_column($allowed_columns, 'date')) {
        return [
            'min_date' => null,
            'max_date' => null,
        ];
    }

    return [
		'min_date' => $wpdb->get_var("SELECT MIN(`date`) FROM `$table`"),
		'max_date' => $wpdb->get_var("SELECT MAX(`date`) FROM `$table`"),
    ];
}

function tsa_get_shot_events_options($request) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table = $wpdb->prefix . 'tsa_shot_events_shot_location_events';

    $teams = $wpdb->get_col("
        SELECT DISTINCT teamAbbrev
        FROM `$table`
        WHERE teamAbbrev <> ''
        ORDER BY teamAbbrev
    ");

    $opponents = $wpdb->get_col("
        SELECT DISTINCT opponent
        FROM (
            SELECT homeAbbrev AS opponent
            FROM `$table`
            WHERE homeAbbrev <> ''

            UNION

            SELECT awayAbbrev AS opponent
            FROM `$table`
            WHERE awayAbbrev <> ''
        ) AS x
        ORDER BY opponent
    ");

	$shot_regions = $wpdb->get_col("
		SELECT DISTINCT `shot_region`
		FROM `$table`
		WHERE `shot_region` <> ''
		ORDER BY `shot_region`
	");

    $shot_types = $wpdb->get_col("
        SELECT DISTINCT shotType
        FROM `$table`
        WHERE shotType <> ''
        ORDER BY shotType
    ");

    return [
        'teams' => $teams,
        'opponents' => $opponents,
        'shot_regions' => $shot_regions,
        'shot_types' => $shot_types,
    ];
}

function tsa_stream_shot_events_csv($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_shot_events_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid shot_events dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;
    $full = intval($request->get_param('full')) === 1;
    $allowed_columns = tsa_get_shot_events_allowed_columns($table);

    $where = [];
    $params = [];

    if (!$full) {
        tsa_apply_shot_events_filters($request, $allowed_columns, $where, $params);
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    if (in_array('date', $allowed_columns, true)) {
        $order_sql = "ORDER BY `date` DESC";
    } elseif (in_array('total_shots', $allowed_columns, true)) {
        $order_sql = "ORDER BY `total_shots` DESC";
    } else {
        $order_sql = "ORDER BY `playerId` ASC";
    }

    $sql = "SELECT * FROM `$table` $where_sql $order_sql";

    $rows = !empty($params)
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);

    if (empty($rows)) {
        wp_die('No rows found for selected filters.');
    }

    header('Content-Type: text/csv; charset=utf-8');

    $filename = $full
        ? "full_shot_events_{$dataset}.csv"
        : "filtered_shot_events_{$dataset}.csv";

    header("Content-Disposition: attachment; filename={$filename}");

    echo "\xEF\xBB\xBF";

    $out = fopen('php://output', 'w');

    fputcsv($out, array_keys($rows[0]), ',', '"', '\\');

    foreach ($rows as $row) {
        fputcsv($out, $row, ',', '"', '\\');
    }

    fclose($out);
    exit;
}



function tsa_get_goalie_dataset_map() {
    return [
        'advanced' => 'tsa_goalie_advanced',
        'bios' => 'tsa_goalie_bios',
        'daysrest' => 'tsa_goalie_daysrest',
        'penaltyshots' => 'tsa_goalie_penaltyshots',
        'savesbystrength' => 'tsa_goalie_savesbystrength',
        'shootout' => 'tsa_goalie_shootout',
        'startedvsrelieved' => 'tsa_goalie_startedvsrelieved',
        'summary' => 'tsa_goalie_summary',
    ];
}

add_action('rest_api_init', function () {
    foreach (tsa_get_goalie_dataset_map() as $dataset => $table_name) {
        register_rest_route('tsa/v1', "/goalie-$dataset", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_goalie_dataset($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/goalie-$dataset-meta", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_get_goalie_date_meta($dataset);
            },
            'permission_callback' => '__return_true',
        ]);

        register_rest_route('tsa/v1', "/goalie-$dataset-csv", [
            'methods' => 'GET',
            'callback' => function ($request) use ($dataset) {
                return tsa_stream_goalie_csv($request, $dataset);
            },
            'permission_callback' => '__return_true',
        ]);
    }

    register_rest_route('tsa/v1', '/goalie-team-options', [
        'methods' => 'GET',
        'callback' => 'tsa_get_goalie_team_options',
        'permission_callback' => '__return_true',
    ]);
});

function tsa_get_goalie_table_name($dataset) {
    $map = tsa_get_goalie_dataset_map();

    if (!isset($map[$dataset])) {
        return null;
    }

    return $map[$dataset];
}

function tsa_get_goalie_allowed_columns($table) {
    global $wpdb;

    $columns = $wpdb->get_results("SHOW COLUMNS FROM `$table`", ARRAY_A);

    return array_map(function ($col) {
        return $col['Field'];
    }, $columns);
}

function tsa_goalie_table_has_column($allowed_columns, $column) {
    return in_array($column, $allowed_columns, true);
}

function tsa_apply_goalie_filters($request, $allowed_columns, &$where, &$params) {
    global $wpdb;

    $teams_raw = sanitize_text_field($request->get_param('teams'));
    $opponents_raw = sanitize_text_field($request->get_param('opponents'));
    $homeRoad = sanitize_text_field($request->get_param('homeRoad'));
    $search = sanitize_text_field($request->get_param('search'));
    $date_single = sanitize_text_field($request->get_param('date_single'));
    $date_start = sanitize_text_field($request->get_param('date_start'));
    $date_end = sanitize_text_field($request->get_param('date_end'));

    if (!empty($teams_raw)) {
        $teams = array_filter(array_map('trim', explode(',', $teams_raw)));

        if (!empty($teams)) {
            $placeholders = implode(',', array_fill(0, count($teams), '%s'));

            if (tsa_goalie_table_has_column($allowed_columns, 'currentTeamAbbrev')) {
                $where[] = "currentTeamAbbrev IN ($placeholders)";
            } elseif (tsa_goalie_table_has_column($allowed_columns, 'teamAbbrev')) {
                $where[] = "teamAbbrev IN ($placeholders)";
            }

            foreach ($teams as $team) {
                $params[] = $team;
            }
        }
    }

    if (tsa_goalie_table_has_column($allowed_columns, 'opponentTeamAbbrev') && !empty($opponents_raw)) {
        $opponents = array_filter(array_map('trim', explode(',', $opponents_raw)));

        if (!empty($opponents)) {
            $placeholders = implode(',', array_fill(0, count($opponents), '%s'));
            $where[] = "opponentTeamAbbrev IN ($placeholders)";

            foreach ($opponents as $opponent) {
                $params[] = $opponent;
            }
        }
    }

    if (tsa_goalie_table_has_column($allowed_columns, 'homeRoad') && !empty($homeRoad)) {
        $where[] = "homeRoad = %s";
        $params[] = $homeRoad;
    }

    if (!empty($search)) {
        $search = trim($search);
        $like = '%' . $wpdb->esc_like($search) . '%';
        $search_parts = [];

        if (tsa_goalie_table_has_column($allowed_columns, 'goalieFullName')) {
            $search_parts[] = "goalieFullName LIKE %s";
            $params[] = $like;
        }

        if (tsa_goalie_table_has_column($allowed_columns, 'lastName')) {
            $search_parts[] = "lastName LIKE %s";
            $params[] = $like;
        }

        if (ctype_digit($search)) {
            foreach (['playerId', 'gameId'] as $col) {
                if (tsa_goalie_table_has_column($allowed_columns, $col)) {
                    $search_parts[] = "CAST($col AS CHAR) LIKE %s";
                    $params[] = $like;
                }
            }
        }

        if (!empty($search_parts)) {
            $where[] = "(" . implode(" OR ", $search_parts) . ")";
        }
    }

    if (tsa_goalie_table_has_column($allowed_columns, 'gameDate')) {
        if (!empty($date_single)) {
            $where[] = "gameDate = %s";
            $params[] = $date_single;
        } elseif (!empty($date_start) && !empty($date_end)) {
            $where[] = "gameDate BETWEEN %s AND %s";
            $params[] = $date_start;
            $params[] = $date_end;
        }
    }
}

function tsa_get_goalie_dataset($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_goalie_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid goalie dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;
    $allowed_columns = tsa_get_goalie_allowed_columns($table);

    $page = max(1, intval($request->get_param('page') ?: 1));
    $size_param = $request->get_param('size') ?: $request->get_param('per_page') ?: 25;
    $per_page = min(100, max(10, intval($size_param)));
    $offset = ($page - 1) * $per_page;

    $where = [];
    $params = [];

    tsa_apply_goalie_filters($request, $allowed_columns, $where, $params);

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $count_sql = "SELECT COUNT(*) FROM $table $where_sql";
    $total = !empty($params)
        ? intval($wpdb->get_var($wpdb->prepare($count_sql, ...$params)))
        : intval($wpdb->get_var($count_sql));

    $last_page = max(1, ceil($total / $per_page));

    $sort_field = tsa_goalie_table_has_column($allowed_columns, 'gameDate')
        ? 'gameDate'
        : (tsa_goalie_table_has_column($allowed_columns, 'goalieFullName') ? 'goalieFullName' : $allowed_columns[0]);

    $sort_dir = tsa_goalie_table_has_column($allowed_columns, 'gameDate') ? 'DESC' : 'ASC';

    $sorters = $request->get_param('sort');

    if (empty($sorters)) {
        $sorters = $request->get_param('sorters');
    }

    if (is_string($sorters)) {
        $decoded = json_decode($sorters, true);
        if (json_last_error() === JSON_ERROR_NONE) {
            $sorters = $decoded;
        }
    }

    if (!empty($sorters) && is_array($sorters)) {
        $first_sorter = $sorters[0] ?? null;

        if (is_array($first_sorter)) {
            if (!empty($first_sorter['field']) && in_array($first_sorter['field'], $allowed_columns, true)) {
                $sort_field = $first_sorter['field'];
            }

            if (!empty($first_sorter['dir']) && strtolower($first_sorter['dir']) === 'asc') {
                $sort_dir = 'ASC';
            } else {
                $sort_dir = 'DESC';
            }
        }
    }

    $order_sql = "ORDER BY `$sort_field` $sort_dir";

    $data_sql = "SELECT *
                 FROM $table
                 $where_sql
                 $order_sql
                 LIMIT %d OFFSET %d";

    $data_params = array_merge($params, [$per_page, $offset]);

    $rows = $wpdb->get_results(
        $wpdb->prepare($data_sql, ...$data_params),
        ARRAY_A
    );

    return [
        'data' => $rows,
        'last_page' => $last_page,
        'total' => $total,
    ];
}

function tsa_get_goalie_date_meta($dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_goalie_table_name($dataset);

    if (!$table_name) {
        return new WP_Error('invalid_dataset', 'Invalid goalie dataset.', ['status' => 400]);
    }

    $table = $wpdb->prefix . $table_name;
    $allowed_columns = tsa_get_goalie_allowed_columns($table);

    if (!tsa_goalie_table_has_column($allowed_columns, 'gameDate')) {
        return [
            'min_date' => null,
            'max_date' => null,
        ];
    }

    return [
        'min_date' => $wpdb->get_var("SELECT MIN(gameDate) FROM $table"),
        'max_date' => $wpdb->get_var("SELECT MAX(gameDate) FROM $table"),
    ];
}

function tsa_stream_goalie_csv($request, $dataset) {
    global $wpdb;

    tsa_set_utf8mb4();

    $table_name = tsa_get_goalie_table_name($dataset);

    if (!$table_name) {
        wp_die('Invalid goalie dataset.');
    }

    $table = $wpdb->prefix . $table_name;
    $allowed_columns = tsa_get_goalie_allowed_columns($table);

    $full = intval($request->get_param('full')) === 1;

    $where = [];
    $params = [];

    if (!$full) {
        tsa_apply_goalie_filters($request, $allowed_columns, $where, $params);
    }

    $where_sql = !empty($where) ? "WHERE " . implode(" AND ", $where) : "";

    $order_col = tsa_goalie_table_has_column($allowed_columns, 'gameDate')
        ? 'gameDate'
        : (tsa_goalie_table_has_column($allowed_columns, 'goalieFullName') ? 'goalieFullName' : $allowed_columns[0]);

    $order_dir = $order_col === 'gameDate' ? 'DESC' : 'ASC';

    $sql = "SELECT * FROM $table $where_sql ORDER BY `$order_col` $order_dir";

    $rows = !empty($params)
        ? $wpdb->get_results($wpdb->prepare($sql, ...$params), ARRAY_A)
        : $wpdb->get_results($sql, ARRAY_A);

    if (empty($rows)) {
        wp_die('No rows found for selected filters.');
    }

    header('Content-Type: text/csv; charset=utf-8');

    $filename = $full
        ? "full_goalie_{$dataset}.csv"
        : "filtered_goalie_{$dataset}.csv";

    header("Content-Disposition: attachment; filename={$filename}");

    echo "\xEF\xBB\xBF";

    $out = fopen('php://output', 'w');

    fputcsv($out, array_keys($rows[0]), ',', '"', '\\');

    foreach ($rows as $row) {
        fputcsv($out, $row, ',', '"', '\\');
    }

    fclose($out);
    exit;
}

function tsa_get_goalie_team_options($request) {
    global $wpdb;

    tsa_set_utf8mb4();

    $bios_table = $wpdb->prefix . 'tsa_goalie_bios';
    $summary_table = $wpdb->prefix . 'tsa_goalie_summary';

    $teams = $wpdb->get_col("
        SELECT team FROM (
            SELECT DISTINCT currentTeamAbbrev AS team
            FROM $bios_table
            WHERE currentTeamAbbrev <> ''

            UNION

            SELECT DISTINCT teamAbbrev AS team
            FROM $summary_table
            WHERE teamAbbrev <> ''

            UNION

            SELECT DISTINCT opponentTeamAbbrev AS team
            FROM $summary_table
            WHERE opponentTeamAbbrev <> ''
        ) AS x
        ORDER BY team
    ");

    return [
        'teams' => $teams,
        'opponents' => $teams,
    ];
}