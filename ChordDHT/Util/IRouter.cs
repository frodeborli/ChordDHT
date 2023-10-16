namespace ChordDHT.Util
{
    public interface IRouter<TContext, THandler>
    {
        public interface Route {
            /**
             * Priority is used for handling the scenario when multiple routes
             * match the context. The highest Priority route will be resolved
             * by the router.
             */
            public bool Predicate(TContext context);
            public THandler Handler { get; }
            public int Priority { get; }
        }

        public void AddRoute(Route route);
        public void AddRoute(IEnumerable<Route> routes);
        public void RemoveRoute(Route route);
        public void RemoveRoute(IEnumerable<Route> routes);
        public THandler? GetHandlerFor(TContext context);
    }
}
