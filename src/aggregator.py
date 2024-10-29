def aggregate_device(device_type_count):
    # 1. Number of devices by device type
    print(f"All device type count:\n\t- {device_type_count}")

    # 2. Top 2 device types by login count
    top_2_devices = device_type_count.most_common(2)
    print("\nTop 2 device types by login count:")
    for device, count in top_2_devices:
        print(f"\t- {device}: {count} logins")


def aggregate_locale(locale_count):
    # 1. Top 3 locales by login count
    top_locales = locale_count.most_common(3)
    print("\nTop 3 locales by login count:")
    for locale, count in top_locales:
        print(f"\t- {locale}: {count} logins")
