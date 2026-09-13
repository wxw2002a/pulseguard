package io.pulseguard.api.web;

import java.util.List;

public record ItemPage<T>(List<T> items) { }
