function resetFilters() {
    const form = document.getElementById('filter-form');
    const currentUrl = new URL(window.location.href);
    form.reset();
    if (currentUrl.search) {
        window.location.href = currentUrl.origin + currentUrl.pathname;
    } else {
        htmx.trigger(form, 'submit');
    }
}